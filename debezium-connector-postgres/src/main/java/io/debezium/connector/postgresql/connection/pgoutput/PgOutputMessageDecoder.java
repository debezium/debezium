/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import static java.util.stream.Collectors.toMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.LogicalDecodingMessage;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.MessageDecoderContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationMessage.NoopMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.TransactionMessage;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.data.Envelope;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * Decodes messages from the PG logical replication plug-in ("pgoutput").
 * See https://www.postgresql.org/docs/10/protocol-logicalrep-message-formats.html for the protocol specification.
 *
 * @author Gunnar Morling
 * @author Chris Cranford
 *
 */
public class PgOutputMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgOutputMessageDecoder.class);
    private static final Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
    private static final byte SPACE = 32;

    private final MessageDecoderContext decoderContext;
    private final PostgresConnection connection;

    private Instant commitTimestamp;

    /**
     * Will be null for a non-transactional decoding message
     */
    private Long transactionId;

    public enum MessageType {
        RELATION,
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE,
        TYPE,
        ORIGIN,
        TRUNCATE,
        LOGICAL_DECODING_MESSAGE;

        public static MessageType forType(char type) {
            switch (type) {
                case 'R':
                    return RELATION;
                case 'B':
                    return BEGIN;
                case 'C':
                    return COMMIT;
                case 'I':
                    return INSERT;
                case 'U':
                    return UPDATE;
                case 'D':
                    return DELETE;
                case 'Y':
                    return TYPE;
                case 'O':
                    return ORIGIN;
                case 'T':
                    return TRUNCATE;
                case 'M':
                    return LOGICAL_DECODING_MESSAGE;
                default:
                    throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    public PgOutputMessageDecoder(MessageDecoderContext decoderContext, PostgresConnection connection) {
        this.decoderContext = decoderContext;
        this.connection = connection;
    }

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, WalPositionLocator walPosition) {
        // Cache position as we're going to peak at the first byte to determine message type
        // We need to reprocess all BEGIN/COMMIT messages regardless.
        int position = buffer.position();
        try {
            MessageType type = MessageType.forType((char) buffer.get());
            LOGGER.trace("Message Type: {}", type);
            switch (type) {
                case TYPE:
                case ORIGIN:
                    // TYPE/ORIGIN
                    // These should be skipped without calling shouldMessageBeSkipped. DBZ-5792
                    LOGGER.trace("{} messages are always skipped without calling shouldMessageBeSkipped", type);
                    return true;
                case TRUNCATE:
                    if (!isTruncateEventsIncluded()) {
                        LOGGER.trace("{} messages are being skipped without calling shouldMessageBeSkipped", type);
                        return true;
                    }
                    // else delegate to super.shouldMessageBeSkipped
                    break;
                default:
                    // call super.shouldMessageBeSkipped for rest of the types
            }
            final boolean candidateForSkipping = super.shouldMessageBeSkipped(buffer, lastReceivedLsn, startLsn, walPosition);
            switch (type) {
                case COMMIT:
                case BEGIN:
                case RELATION:
                    // BEGIN
                    // These types should always be processed due to the nature that they provide
                    // the stream with pertinent per-transaction boundary state we will need to
                    // always cache as we potentially might reprocess the stream from an earlier
                    // LSN point.
                    //
                    // RELATION
                    // These messages are always sent with a lastReceivedLSN=0; and we need to
                    // always accept these to keep per-stream table state cached properly.
                    LOGGER.trace("{} messages are always reprocessed", type);
                    return false;
                default:
                    // INSERT/UPDATE/DELETE/TRUNCATE/LOGICAL_DECODING_MESSAGE
                    // These should be excluded based on the normal behavior, delegating to default method
                    return candidateForSkipping;
            }
        }
        finally {
            // Reset buffer position
            buffer.position(position);
        }
    }

    @Override
    public void processNotEmptyMessage(ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry) throws SQLException, InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            if (!buffer.hasArray()) {
                throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
            }
            final byte[] source = buffer.array();
            // Extend the array by two as we might need to append two chars and set them to space by default
            final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length + 2);
            final int lastPos = content.length - 1;
            content[lastPos - 1] = SPACE;
            content[lastPos] = SPACE;
            LOGGER.trace("Message arrived from database {}", HexConverter.convertToHexString(content));
        }

        final MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
            case BEGIN:
                handleBeginMessage(buffer, processor);
                break;
            case COMMIT:
                handleCommitMessage(buffer, processor);
                break;
            case RELATION:
                handleRelationMessage(buffer, typeRegistry);
                break;
            case LOGICAL_DECODING_MESSAGE:
                handleLogicalDecodingMessage(buffer, processor);
                break;
            case INSERT:
                decodeInsert(buffer, typeRegistry, processor);
                break;
            case UPDATE:
                decodeUpdate(buffer, typeRegistry, processor);
                break;
            case DELETE:
                decodeDelete(buffer, typeRegistry, processor);
                break;
            case TRUNCATE:
                if (isTruncateEventsIncluded()) {
                    decodeTruncate(buffer, typeRegistry, processor);
                }
                else {
                    LOGGER.trace("Message Type {} skipped, not processed.", messageType);
                }
                break;
            default:
                LOGGER.trace("Message Type {} skipped, not processed.", messageType);
                break;
        }
    }

    @Override
    public ChainedLogicalStreamBuilder defaultOptions(ChainedLogicalStreamBuilder builder, Function<Integer, Boolean> hasMinimumServerVersion) {
        builder = builder.withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", decoderContext.getConfig().publicationName());

        // DBZ-4374 Use enum once the driver got updated
        if (hasMinimumServerVersion.apply(140000)) {
            builder = builder.withSlotOption("messages", true);
        }

        return builder;
    }

    private boolean isTruncateEventsIncluded() {
        return !decoderContext.getConfig().getSkippedOperations().contains(Envelope.Operation.TRUNCATE);
    }

    /**
     * Callback handler for the 'B' begin replication message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    volatile private Boolean isEE = null;

    private void handleBeginMessage(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        final Lsn lsn = Lsn.valueOf(buffer.getLong()); // LSN
        this.commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        synchronized (this) {
            if (isEE == null) {
                var cn = connection.connection();
                isEE = false;
                try (var sth = cn.prepareStatement(
                        "select exists(select * from information_schema.routines where routine_name='pgpro_version' and routine_schema='pg_catalog')")) {
                    sth.execute();
                    try (var rs = sth.getResultSet()) {
                        if (rs.next() && rs.getBoolean(1)) {
                            isEE = true;
                            LOGGER.info("Working with PgPro Enterprise");
                        }
                    }
                }
            }
        }
        this.transactionId = isEE ? buffer.getLong() : Integer.toUnsignedLong(buffer.getInt());
        LOGGER.trace("Event: {}", MessageType.BEGIN);
        LOGGER.trace("Final LSN of transaction: {}", lsn);
        LOGGER.trace("Commit timestamp of transaction: {}", commitTimestamp);
        LOGGER.trace("XID of transaction: {}", transactionId);
        processor.process(new TransactionMessage(Operation.BEGIN, transactionId, commitTimestamp));
    }

    /**
     * Callback handler for the 'C' commit replication message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void handleCommitMessage(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        int flags = buffer.get(); // flags, currently unused
        final Lsn lsn = Lsn.valueOf(buffer.getLong()); // LSN of the commit
        final Lsn endLsn = Lsn.valueOf(buffer.getLong()); // End LSN of the transaction
        Instant commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        LOGGER.trace("Event: {}", MessageType.COMMIT);
        LOGGER.trace("Flags: {} (currently unused and most likely 0)", flags);
        LOGGER.trace("Commit LSN: {}", lsn);
        LOGGER.trace("End LSN of transaction: {}", endLsn);
        LOGGER.trace("Commit timestamp of transaction: {}", commitTimestamp);
        processor.process(new TransactionMessage(Operation.COMMIT, transactionId, commitTimestamp));
    }

    /**
     * Callback handler for the 'R' relation replication message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     */
    private void handleRelationMessage(ByteBuffer buffer, TypeRegistry typeRegistry) throws SQLException {
        int relationId = buffer.getInt();
        String schemaName = readString(buffer);
        String tableName = readString(buffer);
        int replicaIdentityId = buffer.get();
        short columnCount = buffer.getShort();

        LOGGER.trace("Event: {}, RelationId: {}, Replica Identity: {}, Columns: {}", MessageType.RELATION, relationId, replicaIdentityId, columnCount);
        LOGGER.trace("Schema: '{}', Table: '{}'", schemaName, tableName);

        // Perform several out-of-bands database metadata queries
        Map<String, Optional<String>> columnDefaults;
        Map<String, Boolean> columnOptionality;
        List<String> primaryKeyColumns;

        final DatabaseMetaData databaseMetadata = connection.connection().getMetaData();
        final TableId tableId = new TableId(null, schemaName, tableName);

        final List<io.debezium.relational.Column> readColumns = getTableColumnsFromDatabase(connection, databaseMetadata, tableId);
        columnDefaults = readColumns.stream()
                .filter(io.debezium.relational.Column::hasDefaultValue)
                .collect(toMap(io.debezium.relational.Column::name, io.debezium.relational.Column::defaultValueExpression));

        columnOptionality = readColumns.stream().collect(toMap(io.debezium.relational.Column::name, io.debezium.relational.Column::isOptional));
        primaryKeyColumns = connection.readPrimaryKeyNames(databaseMetadata, tableId);
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            LOGGER.warn("Primary keys are not defined for table '{}', defaulting to unique indices", tableName);
            primaryKeyColumns = connection.readTableUniqueIndices(databaseMetadata, tableId);
        }

        List<ColumnMetaData> columns = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        for (short i = 0; i < columnCount; ++i) {
            byte flags = buffer.get();
            String columnName = Strings.unquoteIdentifierPart(readString(buffer));
            int columnType = buffer.getInt();
            int attypmod = buffer.getInt();

            final PostgresType postgresType = typeRegistry.get(columnType);
            boolean key = isColumnInPrimaryKey(schemaName, tableName, columnName, primaryKeyColumns);

            Boolean optional = columnOptionality.get(columnName);
            if (optional == null) {
                if (decoderContext.getConfig().getColumnFilter().matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
                    LOGGER.warn("Column '{}' optionality could not be determined, defaulting to true", columnName);
                }
                optional = true;
            }

            final boolean hasDefault = columnDefaults.containsKey(columnName);
            final String defaultValueExpression = columnDefaults.getOrDefault(columnName, Optional.empty()).orElse(null);

            columns.add(new ColumnMetaData(columnName, postgresType, key, optional, hasDefault, defaultValueExpression, attypmod));
            columnNames.add(columnName);
        }

        // Remove any PKs that do not exist as part of this this relation message. This can occur when issuing
        // multiple schema changes in sequence since the lookup of primary keys is an out-of-band procedure, without
        // any logical linkage or context to the point in time the relation message was emitted.
        //
        // Example DDL:
        // ALTER TABLE changepk.test_table DROP COLUMN pk2; -- <- relation message #1
        // ALTER TABLE changepk.test_table ADD COLUMN pk3 SERIAL; -- <- relation message #2
        // ALTER TABLE changepk.test_table ADD PRIMARY KEY(newpk,pk3); -- <- relation message #3
        //
        // Consider the above schema changes. There's a possible temporal ordering where the messages arrive
        // in the replication slot data stream at time `t0`, `t1`, and `t2`. It then takes until `t10` for _this_ method
        // to start processing message #1. At `t10` invoking `connection.readPrimaryKeyNames()` returns the new
        // primary key column, 'pk3', defined by message #3. We must remove this primary key column that came
        // "from the future" (with temporal respect to the current relate message #1) as a best effort attempt
        // to reflect the actual primary key state at time `t0`.
        primaryKeyColumns.retainAll(columnNames);

        Table table = resolveRelationFromMetadata(new PgOutputRelationMetaData(relationId, schemaName, tableName, columns, primaryKeyColumns));
        decoderContext.getSchema().applySchemaChangesForTable(relationId, table);
    }

    private List<io.debezium.relational.Column> getTableColumnsFromDatabase(PostgresConnection connection, DatabaseMetaData databaseMetadata, TableId tableId)
            throws SQLException {
        List<io.debezium.relational.Column> readColumns = new ArrayList<>();
        try {
            try (ResultSet columnMetadata = databaseMetadata.getColumns(null, tableId.schema(), tableId.table(), null)) {
                while (columnMetadata.next()) {
                    connection.readColumnForDecoder(columnMetadata, tableId, decoderContext.getConfig().getColumnFilter())
                            .ifPresent(readColumns::add);
                }
            }
        }
        catch (SQLException e) {
            LOGGER.error("Failed to read column metadata for '{}.{}'", tableId.schema(), tableId.table());
            throw e;
        }

        return readColumns;
    }

    private boolean isColumnInPrimaryKey(String schemaName, String tableName, String columnName, List<String> primaryKeyColumns) {
        // todo (DBZ-766) - Discuss this logic with team as there may be a better way to handle this
        // Personally I think its sufficient enough to resolve the PK based on the out-of-bands call
        // and should any test fail due to this it should be rewritten or excluded from the pgoutput
        // scope.
        //
        // In RecordsStreamProducerIT#shouldReceiveChangesForInsertsIndependentOfReplicaIdentity, we have
        // a situation where the table is replica identity full, the primary key is dropped but the replica
        // identity is kept and later the replica identity is changed to default. In order to support this
        // use case, the following abides by these rules:
        //
        if (!primaryKeyColumns.isEmpty() && primaryKeyColumns.contains(columnName)) {
            return true;
        }
        else if (primaryKeyColumns.isEmpty()) {
            // The table's metadata was either not fetched or table no longer has a primary key
            // Lets attempt to use the known schema primary key configuration as a fallback
            Table existingTable = decoderContext.getSchema().tableFor(new TableId(null, schemaName, tableName));
            if (existingTable != null && existingTable.primaryKeyColumnNames().contains(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Callback handler for the 'I' insert replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    private void decodeInsert(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get(); // Always 'N" for inserts

        LOGGER.trace("Event: {}, Relation Id: {}, Tuple Type: {}", MessageType.INSERT, relationId, tupleType);

        Optional<Table> resolvedTable = resolveRelation(relationId);

        // non-captured table
        if (!resolvedTable.isPresent()) {
            processor.process(new NoopMessage(transactionId, commitTimestamp));
        }
        else {
            Table table = resolvedTable.get();
            List<Column> columns = resolveColumnsFromStreamTupleData(buffer, typeRegistry, table);
            processor.process(new PgOutputReplicationMessage(
                    Operation.INSERT,
                    table.id().toDoubleQuotedString(),
                    commitTimestamp,
                    transactionId,
                    null,
                    columns));
        }
    }

    /**
     * Callback handler for the 'U' update replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    private void decodeUpdate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        int relationId = buffer.getInt();

        LOGGER.trace("Event: {}, RelationId: {}", MessageType.UPDATE, relationId);

        Optional<Table> resolvedTable = resolveRelation(relationId);

        // non-captured table
        if (!resolvedTable.isPresent()) {
            processor.process(new NoopMessage(transactionId, commitTimestamp));
        }
        else {
            Table table = resolvedTable.get();

            // When reading the tuple-type, we could get 3 different values, 'O', 'K', or 'N'.
            // 'O' (Optional) - States the following tuple-data is the key, only for replica identity index configs.
            // 'K' (Optional) - States the following tuple-data is the old tuple, only for replica identity full configs.
            //
            // 'N' (Not-Optional) - States the following tuple-data is the new tuple.
            // This is always present.
            List<Column> oldColumns = null;
            char tupleType = (char) buffer.get();
            if ('O' == tupleType || 'K' == tupleType) {
                oldColumns = resolveColumnsFromStreamTupleData(buffer, typeRegistry, table);
                // Read the 'N' tuple type
                // This is necessary so the stream position is accurate for resolving the column tuple data
                tupleType = (char) buffer.get();
            }

            List<Column> columns = resolveColumnsFromStreamTupleData(buffer, typeRegistry, table);
            processor.process(new PgOutputReplicationMessage(
                    Operation.UPDATE,
                    table.id().toDoubleQuotedString(),
                    commitTimestamp,
                    transactionId,
                    oldColumns,
                    columns));
        }
    }

    /**
     * Callback handler for the 'D' delete replication stream message.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor The replication message processor
     */
    private void decodeDelete(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        int relationId = buffer.getInt();

        char tupleType = (char) buffer.get();

        LOGGER.trace("Event: {}, RelationId: {}, Tuple Type: {}", MessageType.DELETE, relationId, tupleType);

        Optional<Table> resolvedTable = resolveRelation(relationId);

        // non-captured table
        if (!resolvedTable.isPresent()) {
            processor.process(new NoopMessage(transactionId, commitTimestamp));
        }
        else {
            Table table = resolvedTable.get();
            List<Column> columns = resolveColumnsFromStreamTupleData(buffer, typeRegistry, table);
            processor.process(new PgOutputReplicationMessage(
                    Operation.DELETE,
                    table.id().toDoubleQuotedString(),
                    commitTimestamp,
                    transactionId,
                    columns,
                    null));
        }
    }

    /**
     * Callback handler for the 'T' truncate replication stream message.
     *
     * @param buffer       The replication stream buffer
     * @param typeRegistry The postgres type registry
     * @param processor    The replication message processor
     */
    private void decodeTruncate(ByteBuffer buffer, TypeRegistry typeRegistry, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        // As of PG11, the Truncate message format is as described:
        // Byte Message Type (Always 'T')
        // Int32 number of relations described by the truncate message
        // Int8 flags for truncate; 1=CASCADE, 2=RESTART IDENTITY
        // Int32[] Array of number of relation ids
        //
        // In short this message tells us how many relations are impacted by the truncate
        // call, whether its cascaded or not and then all table relation ids involved.
        // It seems the protocol guarantees to send the most up-to-date `R` relation
        // messages for the tables prior to the `T` truncation message, even if in the
        // same session a `R` message was followed by an insert/update/delete message.

        int numberOfRelations = buffer.getInt();
        int optionBits = buffer.get();
        // ignored / unused
        List<String> truncateOptions = getTruncateOptions(optionBits);
        int[] relationIds = new int[numberOfRelations];
        for (int i = 0; i < numberOfRelations; i++) {
            relationIds[i] = buffer.getInt();
        }

        List<Table> tables = new ArrayList<>();
        for (int relationId : relationIds) {
            Optional<Table> resolvedTable = resolveRelation(relationId);
            resolvedTable.ifPresent(tables::add);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Event: {}, RelationIds: {}, OptionBits: {}", MessageType.TRUNCATE, Arrays.toString(relationIds), optionBits);
        }

        int noOfResolvedTables = tables.size();
        for (int i = 0; i < noOfResolvedTables; i++) {
            Table table = tables.get(i);
            boolean lastTableInTruncate = (i + 1) == noOfResolvedTables;
            processor.process(new PgOutputTruncateReplicationMessage(
                    Operation.TRUNCATE,
                    table.id().toDoubleQuotedString(),
                    commitTimestamp,
                    transactionId,
                    lastTableInTruncate));
        }
    }

    /**
     * Convert truncate option bits to postgres syntax truncate options
     *
     * @param flag truncate option bits
     * @return truncate flags
     */
    private List<String> getTruncateOptions(int flag) {
        switch (flag) {
            case 1:
                return Collections.singletonList("CASCADE");
            case 2:
                return Collections.singletonList("RESTART IDENTITY");
            case 3:
                return Arrays.asList("RESTART IDENTITY", "CASCADE");
            default:
                return null;
        }
    }

    /**
     * Callback handler for the 'M' logical decoding message
     *
     * @param buffer       The replication stream buffer
     * @param processor    The replication message processor
     */
    private void handleLogicalDecodingMessage(ByteBuffer buffer, ReplicationMessageProcessor processor)
            throws SQLException, InterruptedException {
        // As of PG14, the MESSAGE message format is as described:
        // Byte1 Always 'M'
        // Int32 Xid of the transaction (only present for streamed transactions in protocol version 2).
        // Int8 flags; Either 0 for no flags or 1 if the logical decoding message is transactional.
        // Int64 The LSN of the logical decoding message
        // String The prefix of the logical decoding message.
        // Int32 Length of the content.
        // Byten The content of the logical decoding message.

        boolean isTransactional = buffer.get() == 1;
        final Lsn lsn = Lsn.valueOf(buffer.getLong());
        String prefix = readString(buffer);
        int contentLength = buffer.getInt();
        byte[] content = new byte[contentLength];
        buffer.get(content);

        // non-transactional messages do not have xids or commitTimestamps
        if (!isTransactional) {
            transactionId = null;
            commitTimestamp = null;
        }

        LOGGER.trace("Event: {}", MessageType.LOGICAL_DECODING_MESSAGE);
        LOGGER.trace("Commit LSN: {}", lsn);
        LOGGER.trace("Commit timestamp of transaction: {}", commitTimestamp);
        LOGGER.trace("XID of transaction: {}", transactionId);
        LOGGER.trace("Transactional: {}", isTransactional);
        LOGGER.trace("Prefix: {}", prefix);

        processor.process(new LogicalDecodingMessage(
                Operation.MESSAGE,
                commitTimestamp,
                transactionId,
                isTransactional,
                prefix,
                content));
    }

    /**
     * Resolves a given replication message relation identifier to a {@link Table}.
     *
     * @param relationId The replication message stream's relation identifier
     * @return table resolved from a prior relation message or direct lookup from the schema
     * or empty when the table is filtered
     */
    private Optional<Table> resolveRelation(int relationId) {
        return Optional.ofNullable(decoderContext.getSchema().tableFor(relationId));
    }

    /**
     * Constructs a {@link Table} based on the supplied {@link PgOutputRelationMetaData}.
     *
     * @param metadata The relation metadata collected from previous 'R' replication stream messages
     * @return table based on a prior replication relation message
     */
    private Table resolveRelationFromMetadata(PgOutputRelationMetaData metadata) {
        List<io.debezium.relational.Column> columns = new ArrayList<>();
        for (ColumnMetaData columnMetadata : metadata.getColumns()) {
            ColumnEditor editor = io.debezium.relational.Column.editor()
                    .name(columnMetadata.getColumnName())
                    .jdbcType(columnMetadata.getPostgresType().getRootType().getJdbcId())
                    .nativeType(columnMetadata.getPostgresType().getRootType().getOid())
                    .optional(columnMetadata.isOptional())
                    .type(columnMetadata.getPostgresType().getName(), columnMetadata.getTypeName())
                    .length(columnMetadata.getLength())
                    .scale(columnMetadata.getScale());

            if (columnMetadata.hasDefaultValue()) {
                editor.defaultValueExpression(columnMetadata.getDefaultValueExpression());
            }

            columns.add(editor.create());
        }

        Table table = Table.editor()
                .addColumns(columns)
                .setPrimaryKeyNames(metadata.getPrimaryKeyNames())
                .tableId(metadata.getTableId())
                .create();

        LOGGER.trace("Resolved '{}' as '{}'", table.id(), table);

        return table;
    }

    /**
     * Reads the replication stream up to the next null-terminator byte and returns the contents as a string.
     *
     * @param buffer The replication stream buffer
     * @return string read from the replication stream
     */
    private static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    /**
     * Reads the replication stream where the column stream specifies a length followed by the value.
     *
     * @param buffer The replication stream buffer
     * @return the column value as a string read from the replication stream
     */
    private static String readColumnValueAsString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] value = new byte[length];
        buffer.get(value, 0, length);
        return new String(value, Charset.forName("UTF-8"));
    }

    /**
     * Resolve the replication stream's tuple data to a list of replication message columns.
     *
     * @param buffer The replication stream buffer
     * @param typeRegistry The database type registry
     * @param table The database table
     * @return list of replication message columns
     */
    private static List<Column> resolveColumnsFromStreamTupleData(ByteBuffer buffer, TypeRegistry typeRegistry, Table table) {
        // Read number of the columns
        short numberOfColumns = buffer.getShort();

        List<Column> columns = new ArrayList<>(numberOfColumns);
        for (short i = 0; i < numberOfColumns; ++i) {

            final io.debezium.relational.Column column = table.columns().get(i);
            final String columnName = column.name();
            final String typeName = column.typeName();
            final PostgresType columnType = typeRegistry.get(typeName);
            final String typeExpression = column.typeExpression();
            final boolean optional = column.isOptional();

            final Column replicationMessageColumn;

            // Read the sub-message type
            // 't' : Value is represented as text
            // 'u' : An unchanged TOAST-ed value, actual value is not sent.
            // 'n' : Value is null.
            char type = (char) buffer.get();
            if (type == 't') {
                final String valueStr = readColumnValueAsString(buffer);
                replicationMessageColumn = new AbstractReplicationMessageColumn(columnName, columnType, typeExpression, optional) {
                    @Override
                    public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                        return PgOutputReplicationMessage.getValue(columnName, columnType, typeExpression, valueStr, connection, includeUnknownDatatypes,
                                typeRegistry);
                    }

                    @Override
                    public String toString() {
                        return columnName + "(" + typeExpression + ")=" + valueStr;
                    }
                };
            }
            else if (type == 'u') {
                replicationMessageColumn = new UnchangedToastedReplicationMessageColumn(columnName, columnType, typeExpression, optional) {
                    @Override
                    public String toString() {
                        return columnName + "(" + typeExpression + ") - Unchanged toasted column";
                    }
                };
            }
            else if (type == 'n') {
                replicationMessageColumn = new AbstractReplicationMessageColumn(columnName, columnType, typeExpression, true) {
                    @Override
                    public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                        return null;
                    }
                };
            }
            else {
                replicationMessageColumn = null;
                LOGGER.trace("Unsupported type '{}' for column: '{}'", type, column);
            }

            if (replicationMessageColumn != null) {
                columns.add(replicationMessageColumn);
                LOGGER.trace("Column: {}", replicationMessageColumn);
            }
        }

        return columns;
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
    }
}

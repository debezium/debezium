/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.MessageDecoderConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationMessage.NoopMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.TransactionMessage;
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

    private Instant commitTimestamp;
    private int transactionId;

    private final MessageDecoderConfig config;

    public enum MessageType {
        RELATION,
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE,
        TYPE,
        ORIGIN,
        TRUNCATE;

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
                default:
                    throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    public PgOutputMessageDecoder(MessageDecoderConfig config) {
        this.config = config;
    }

    @Override
    public boolean shouldMessageBeSkipped(ByteBuffer buffer, Long lastReceivedLsn, Long startLsn, boolean skipFirstFlushRecord) {
        // Cache position as we're going to peak at the first byte to determine message type
        // We need to reprocess all BEGIN/COMMIT messages regardless.
        int position = buffer.position();
        try {
            MessageType type = MessageType.forType((char) buffer.get());
            LOGGER.trace("Message Type: {}", type);
            switch (type) {
                case TRUNCATE:
                    // @formatter:off
                    // For now we plan to gracefully skip TRUNCATE messages.
                    // We may decide in the future that these may be emitted differently, see DBZ-1052.
                    //
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
                    // @formatter:on
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
                    // INSERT/UPDATE/DELETE/TYPE/ORIGIN
                    // These should be excluded based on the normal behavior, delegating to default method
                    return super.shouldMessageBeSkipped(buffer, lastReceivedLsn, startLsn, skipFirstFlushRecord);
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
            case INSERT:
                decodeInsert(buffer, typeRegistry, processor);
                break;
            case UPDATE:
                decodeUpdate(buffer, typeRegistry, processor);
                break;
            case DELETE:
                decodeDelete(buffer, typeRegistry, processor);
                break;
            default:
                LOGGER.trace("Message Type {} skipped, not processed.", messageType);
                break;
        }
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return builder.withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", config.getPublicationName());
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    /**
     * Callback handler for the 'B' begin replication message.
     *
     * @param buffer The replication stream buffer
     * @param processor The replication message processor
     */
    private void handleBeginMessage(ByteBuffer buffer, ReplicationMessageProcessor processor) throws SQLException, InterruptedException {
        long lsn = buffer.getLong(); // LSN
        this.commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        this.transactionId = buffer.getInt();
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
        long lsn = buffer.getLong(); // LSN of the commit
        long endLsn = buffer.getLong(); // End LSN of the transaction
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
        Map<String, Boolean> columnOptionality;
        Set<String> primaryKeyColumns;
        try (final PostgresConnection connection = new PostgresConnection(config.getConfiguration())) {
            final DatabaseMetaData databaseMetadata = connection.connection().getMetaData();
            columnOptionality = getTableColumnOptionalityFromDatabase(databaseMetadata, schemaName, tableName);
            primaryKeyColumns = getTablePrimaryKeyColumnNamesFromDatabase(databaseMetadata, schemaName, tableName);
            if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
                primaryKeyColumns = new HashSet<>(connection.readTableUniqueIndices(databaseMetadata, new TableId(null, schemaName, tableName)));
            }
        }

        List<ColumnMetaData> columns = new ArrayList<>();
        for (short i = 0; i < columnCount; ++i) {
            byte flags = buffer.get();
            String columnName = Strings.unquoteIdentifierPart(readString(buffer));
            int columnType = buffer.getInt();
            int attypmod = buffer.getInt();

            final PostgresType postgresType = typeRegistry.get(columnType);
            boolean key = isColumnInPrimaryKey(schemaName, tableName, columnName, primaryKeyColumns);

            Boolean optional = columnOptionality.get(columnName);
            if (optional == null) {
                LOGGER.warn("Column '{}' optionality could not be determined, defaulting to true", columnName);
                optional = true;
            }

            columns.add(new ColumnMetaData(columnName, postgresType, key, optional, attypmod));
        }

        Table table = resolveRelationFromMetadata(new PgOutputRelationMetaData(relationId, schemaName, tableName, columns));
        config.getSchema().applySchemaChangesForTable(relationId, table);
    }

    private Map<String, Boolean> getTableColumnOptionalityFromDatabase(DatabaseMetaData databaseMetadata, String schemaName, String tableName) {
        Map<String, Boolean> columnOptionality = new HashMap<>();
        try {
            try (ResultSet resultSet = databaseMetadata.getColumns(null, schemaName, tableName, null)) {
                while (resultSet.next()) {
                    columnOptionality.put(resultSet.getString("COLUMN_NAME"), resultSet.getString("IS_NULLABLE").equals("YES"));
                }
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to read column optionality metadata for '{}.{}'", schemaName, tableName);
            // todo: DBZ-766 Should this throw the exception or just log the warning?
        }
        return columnOptionality;
    }

    private Set<String> getTablePrimaryKeyColumnNamesFromDatabase(DatabaseMetaData databaseMetadata, String schemaName, String tableName) {
        Set<String> primaryKeyColumns = new HashSet<>();
        try (ResultSet resultSet = databaseMetadata.getPrimaryKeys(null, schemaName, tableName)) {
            while (resultSet.next()) {
                primaryKeyColumns.add(resultSet.getString("COLUMN_NAME"));
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to read table {}.{} primary keys", schemaName, tableName);
        }
        return primaryKeyColumns;
    }

    private boolean isColumnInPrimaryKey(String schemaName, String tableName, String columnName, Set<String> primaryKeyColumns) {
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
            Table existingTable = config.getSchema().tableFor(new TableId(null, schemaName, tableName));
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
     * Resolves a given replication message relation identifier to a {@link Table}.
     *
     * @param relationId The replication message stream's relation identifier
     * @return table resolved from a prior relation message or direct lookup from the schema
     * or empty when the table is filtered
     */
    private Optional<Table> resolveRelation(int relationId) {
        return Optional.ofNullable(config.getSchema().tableFor(relationId));
    }

    /**
     * Constructs a {@link Table} based on the supplied {@link PgOutputRelationMetaData}.
     *
     * @param metadata The relation metadata collected from previous 'R' replication stream messages
     * @return table based on a prior replication relation message
     */
    private Table resolveRelationFromMetadata(PgOutputRelationMetaData metadata) {
        List<String> pkColumnNames = new ArrayList<>();
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

            columns.add(editor.create());

            if (columnMetadata.isKey()) {
                pkColumnNames.add(columnMetadata.getColumnName());
            }
        }

        Table table = Table.editor()
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
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

            // Read the sub-message type
            // 't' : Value is represented as text
            // 'u' : An unchanged TOAST-ed value, actual value is not sent.
            // 'n' : Value is null.
            char type = (char) buffer.get();
            if (type == 't') {
                final String valueStr = readColumnValueAsString(buffer);
                columns.add(
                        new AbstractReplicationMessageColumn(columnName, columnType, typeExpression, optional, true) {
                            @Override
                            public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                                return PgOutputReplicationMessage.getValue(columnName, columnType, typeExpression, valueStr, connection, includeUnknownDatatypes,
                                        typeRegistry);
                            }

                            @Override
                            public String toString() {
                                return columnName + "(" + typeExpression + ")=" + valueStr;
                            }
                        });
            }
            else if (type == 'u') {
                columns.add(
                        new UnchangedToastedReplicationMessageColumn(columnName, columnType, typeExpression, optional, true) {
                            @Override
                            public String toString() {
                                return columnName + "(" + typeExpression + ") - Unchanged toasted column";
                            }
                        });
            }
            else if (type == 'n') {
                columns.add(
                        new AbstractReplicationMessageColumn(columnName, columnType, typeExpression, true, true) {
                            @Override
                            public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                                return null;
                            }
                        });
            }
        }

        columns.forEach(c -> LOGGER.trace("Column: {}", c));
        return columns;
    }
}

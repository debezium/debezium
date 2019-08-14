/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);

    private final PostgresConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresOffsetContext offsetContext;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();
    private Long lastCompletelyProcessedLsn;
    private final Snapshotter snapshotter;
    private final Metronome pauseNoMessage;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter, PostgresOffsetContext offsetContext, PostgresConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, PostgresSchema schema, PostgresTaskContext taskContext, ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = (offsetContext != null) ? offsetContext :
            PostgresOffsetContext.initialContext(connectorConfig, connection, clock);
        pauseNoMessage = Metronome.sleeper(taskContext.getConfig().getPollInterval(), Clock.SYSTEM);
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Streaming is not enabled in currect configuration");
            return;
        }

        try {
            if (offsetContext.hasLastKnownPosition()) {
                // start streaming from the last recorded position in the offset
                Long lsn = offsetContext.lsn();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("retrieved latest position from stored offset '{}'", ReplicationConnection.format(lsn));
                }
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn));
            } else {
                LOGGER.info("no previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                replicationStream.compareAndSet(null, replicationConnection.startStreaming());
            }

            // refresh the schema so we have a latest view of the DB tables
            taskContext.refreshSchema(connection, true);

            this.lastCompletelyProcessedLsn = offsetContext.lsn();

            final ReplicationStream stream = this.replicationStream.get();
            while (context.isRunning()) {
                stream.readPending(message -> {
                    final Long lsn = stream.lastReceivedLsn();
                    if (message == null) {
                        LOGGER.trace("Received empty message");
                        lastCompletelyProcessedLsn = lsn;
                        pauseNoMessage.pause();
                        return;
                    }
                    if (message.isLastEventForLsn()) {
                        lastCompletelyProcessedLsn = lsn;
                    }

                    final TableId tableId = PostgresSchema.parse(message.getTable());
                    Objects.requireNonNull(tableId);

                    offsetContext.updateWalPosition(lsn, lastCompletelyProcessedLsn, message.getCommitTime(), message.getTransactionId(), tableId, taskContext.getSlotXmin(connection));
                    dispatcher
                        .dispatchDataChangeEvent(
                                tableId,
                                new PostgresChangeRecordEmitter(
                                        offsetContext,
                                        clock,
                                        connectorConfig,
                                        schema,
                                        connection,
                                        message
                                )
                        );
                });
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            if (replicationConnection != null) {
                LOGGER.debug("stopping streaming...");
                //TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
                //replicationStream.close();
                // close the connection - this should also disconnect the current stream even if it's blocking
                try {
                    replicationConnection.close();
                }
                catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        try {
            ReplicationStream replicationStream = this.replicationStream.get();
            final Long lsn = (Long) offset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);

            if (replicationStream != null && lsn != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Flushing LSN to server: {}", LogSequenceNumber.valueOf(lsn));
                }
                // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
                replicationStream.flushLsn(lsn);
            }
            else {
                LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private void process(ReplicationMessage message, Long lsn, BlockingConsumer<ChangeEvent> consumer) throws SQLException, InterruptedException {
        // in some cases we can get null if PG gives us back a message earlier than the latest reported flushed LSN.
        // WAL2JSON can also send empty changes for DDL, materialized views, etc. and the heartbeat still needs to fire.
        if (message == null) {
            LOGGER.trace("Received empty message");
            lastCompletelyProcessedLsn = lsn;
            // TODO heartbeat
            return;
        }
        if (message.isLastEventForLsn()) {
            lastCompletelyProcessedLsn = lsn;
        }

        TableId tableId = PostgresSchema.parse(message.getTable());
        assert tableId != null;

        // update the source info with the coordinates for this message
        Instant commitTime = message.getCommitTime();
        long txId = message.getTransactionId();
        offsetContext.updateWalPosition(lsn, null, commitTime, txId, tableId, taskContext.getSlotXmin(connection));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received new message at position {}\n{}", ReplicationConnection.format(lsn), message);
        }

        TableSchema tableSchema = tableSchemaFor(tableId);
        if (tableSchema != null) {

            ReplicationMessage.Operation operation = message.getOperation();
            switch (operation) {
                case INSERT: {
                    Object[] row = columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                    generateCreateRecord(tableId, row, consumer);
                    break;
                }
                case UPDATE: {
                    Object[] newRow = columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                    Object[] oldRow = columnValues(message.getOldTupleList(), tableId, false, message.hasTypeMetadata());
                    generateUpdateRecord(tableId, oldRow, newRow, consumer);
                    break;
                }
                case DELETE: {
                    Object[] row = columnValues(message.getOldTupleList(), tableId, false, message.hasTypeMetadata());
                    generateDeleteRecord(tableId, row, consumer);
                    break;
                }
                default: {
                   LOGGER.warn("unknown message operation: {}", operation);
                }
            }
        }

        if (message.isLastEventForLsn()) {
            // TODO heartbeat
        }
    }

    protected void generateCreateRecord(TableId tableId, Object[] rowData, BlockingConsumer<ChangeEvent> recordConsumer) throws InterruptedException {
        if (rowData == null || rowData.length == 0) {
            LOGGER.warn("no new values found for table '{}' from update message at '{}'; skipping record", tableId, offsetContext);
            return;
        }
        TableSchema tableSchema = schema.schemaFor(tableId);
        assert tableSchema != null;
        Object key = tableSchema.keyFromColumnData(rowData);
        LOGGER.trace("key value is: {}", key);
        Struct value = tableSchema.valueFromColumnData(rowData);
        if (value == null) {
            LOGGER.warn("no values found for table '{}' from create message at '{}'; skipping record", tableId, offsetContext);
            return;
        }
        Schema keySchema = tableSchema.keySchema();
        Map<String, ?> partition = offsetContext.getPartition();
        Map<String, ?> offset = offsetContext.getOffset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = tableSchema.getEnvelopeSchema();

        SourceRecord record = new SourceRecord(partition, offset, topicName, null, keySchema, key, envelope.schema(),
                                               envelope.create(value, offsetContext.getSourceInfo(), clock.currentTimeInMillis()));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending create event '{}' to topic '{}'", record, topicName);
        }
        recordConsumer.accept(new ChangeEvent(record, lastCompletelyProcessedLsn));
    }

    protected void generateUpdateRecord(TableId tableId, Object[] oldRowData, Object[] newRowData,
                                        BlockingConsumer<ChangeEvent> recordConsumer) throws InterruptedException {
        if (newRowData == null || newRowData.length == 0) {
            LOGGER.warn("no values found for table '{}' from update message at '{}'; skipping record" , tableId, offsetContext);
            return;
        }
        Schema oldKeySchema = null;
        Struct oldValue = null;
        Object oldKey = null;

        TableSchema tableSchema = schema.schemaFor(tableId);
        assert tableSchema != null;

        if (oldRowData != null && oldRowData.length > 0) {
            oldKey = tableSchema.keyFromColumnData(oldRowData);
            oldKeySchema = tableSchema.keySchema();
            oldValue = tableSchema.valueFromColumnData(oldRowData);
        }

        Object newKey = tableSchema.keyFromColumnData(newRowData);
        Struct newValue = tableSchema.valueFromColumnData(newRowData);

        Schema newKeySchema = tableSchema.keySchema();
        Map<String, ?> partition =  offsetContext.getPartition();
        Map<String, ?> offset = offsetContext.getOffset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = tableSchema.getEnvelopeSchema();
        Struct source = offsetContext.getSourceInfo();

        if (oldKey != null && !Objects.equals(oldKey, newKey)) {
            // the primary key has changed, so we need to send a DELETE followed by a CREATE

            // then send a delete event for the old key ...
            ChangeEvent changeEvent = new ChangeEvent(
                    new SourceRecord(
                            partition, offset, topicName, null, oldKeySchema, oldKey, envelope.schema(),
                            envelope.delete(oldValue, source, clock.currentTimeInMillis())),
                    lastCompletelyProcessedLsn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sending delete event '{}' to topic '{}'", changeEvent.getRecord(), topicName);
            }
            recordConsumer.accept(changeEvent);

            if (taskContext.config().isEmitTombstoneOnDelete()) {
                // send a tombstone event (null value) for the old key so it can be removed from the Kafka log eventually...
                changeEvent = new ChangeEvent(
                        new SourceRecord(partition, offset, topicName, null, oldKeySchema, oldKey, null, null),
                        lastCompletelyProcessedLsn);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("sending tombstone event '{}' to topic '{}'", changeEvent.getRecord(), topicName);
                }
                recordConsumer.accept(changeEvent);
            }

            // then send a create event for the new key...
            changeEvent = new ChangeEvent(
                    new SourceRecord(
                            partition, offset, topicName, null, newKeySchema, newKey, envelope.schema(),
                            envelope.create(newValue, source, clock.currentTimeInMillis())),
                    lastCompletelyProcessedLsn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sending create event '{}' to topic '{}'", changeEvent.getRecord(), topicName);
            }
            recordConsumer.accept(changeEvent);
        } else {
            SourceRecord record = new SourceRecord(partition, offset, topicName, null,
                                                   newKeySchema, newKey, envelope.schema(),
                                                   envelope.update(oldValue, newValue, source, clock.currentTimeInMillis()));
            recordConsumer.accept(new ChangeEvent(record, lastCompletelyProcessedLsn));
        }
    }

    protected void generateDeleteRecord(TableId tableId, Object[] oldRowData, BlockingConsumer<ChangeEvent> recordConsumer) throws InterruptedException {
        if (oldRowData == null || oldRowData.length == 0) {
            LOGGER.warn("no values found for table '{}' from delete message at '{}'; skipping record" , tableId, offsetContext);
            return;
        }
        TableSchema tableSchema = schema.schemaFor(tableId);
        assert tableSchema != null;
        Object key = tableSchema.keyFromColumnData(oldRowData);
        Struct value = tableSchema.valueFromColumnData(oldRowData);
        if (value == null) {
            LOGGER.warn("ignoring delete message for table '{}' because it does not have a primary key defined and replica identity for the table is not FULL", tableId);
            return;
        }
        Schema keySchema = tableSchema.keySchema();
        Map<String, ?> partition =  offsetContext.getPartition();
        Map<String, ?> offset = offsetContext.getOffset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = tableSchema.getEnvelopeSchema();

        // create the regular delete record
        ChangeEvent changeEvent = new ChangeEvent(
                new SourceRecord(
                        partition, offset, topicName, null,
                        keySchema, key, envelope.schema(),
                        envelope.delete(value, offsetContext.getSourceInfo(), clock.currentTimeInMillis())),
                lastCompletelyProcessedLsn);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("sending delete event '{}' to topic '{}'", changeEvent.getRecord(), topicName);
        }
        recordConsumer.accept(changeEvent);

        // And send a tombstone event (null value) for the old key so it can be removed from the Kafka log eventually...
        if (taskContext.config().isEmitTombstoneOnDelete()) {
            changeEvent = new ChangeEvent(
                    new SourceRecord(partition, offset, topicName, null, keySchema, key, null, null),
                    lastCompletelyProcessedLsn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("sending tombstone event '{}' to topic '{}'", changeEvent.getRecord(), topicName);
            }
            recordConsumer.accept(changeEvent);
        }
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId, boolean refreshSchemaIfChanged, boolean metadataInMessage)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        Table table = schema.tableFor(tableId);
        assert table != null;

        // check if we need to refresh our local schema due to DB schema changes for this table
        if (refreshSchemaIfChanged && schemaChanged(columns, table, metadataInMessage)) {
            try (final PostgresConnection connection = taskContext.createConnection()) {
                // Refresh the schema so we get information about primary keys
                schema.refresh(connection, tableId, taskContext.config().skipRefreshSchemaOnMissingToastableData());
                // Update the schema with metadata coming from decoder message
                if (metadataInMessage) {
                    schema.refresh(tableFromFromMessage(columns, schema.tableFor(tableId)));
                }
                table = schema.tableFor(tableId);
            }
        }

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columns.size() < schemaColumns.size() ? schemaColumns.size() : columns.size()];

        for (ReplicationMessage.Column column : columns) {
            //DBZ-298 Quoted column names will be sent like that in messages, but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            final Column tableColumn = table.columnWithName(columnName);
            if (tableColumn == null) {
                LOGGER.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            int position = tableColumn.position() - 1;
            if (position < 0 || position >= values.length) {
                LOGGER.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            values[position] = column.getValue(this::typeResolverConnection, taskContext.config().includeUnknownDatatypes());
        }

        return values;
    }

    private boolean schemaChanged(List<ReplicationMessage.Column> columns, Table table, boolean metadataInMessage) {
        int tableColumnCount = table.columns().size();
        int replicationColumnCount = columns.size();

        boolean msgHasMissingColumns = tableColumnCount > replicationColumnCount;

        if (msgHasMissingColumns && taskContext.config().skipRefreshSchemaOnMissingToastableData()) {
            // if we are ignoring missing toastable data for the purpose of schema sync, we need to modify the
            // hasMissingColumns boolean to account for this. If there are untoasted columns missing from the replication
            // message, we'll still have missing columns and thus require a schema refresh. However, we can /possibly/
            // avoid the refresh if there are only toastable columns missing from the message.
            msgHasMissingColumns = hasMissingUntoastedColumns(table, columns);
        }

        boolean msgHasAdditionalColumns = tableColumnCount < replicationColumnCount;

        if (msgHasMissingColumns || msgHasAdditionalColumns) {
            // the table metadata has less or more columns than the event, which means the table structure has changed,
            // so we need to trigger a refresh...
            LOGGER.info("Different column count {} present in the server message as schema in memory contains {}; refreshing table schema",
                        replicationColumnCount,
                        tableColumnCount);
            return true;
        }

        // go through the list of columns from the message to figure out if any of them are new or have changed their type based
        // on what we have in the table metadata....
        return columns.stream().filter(message -> {
            String columnName = message.getName();
            Column column = table.columnWithName(columnName);
            if (column == null) {
                LOGGER.info("found new column '{}' present in the server message which is not part of the table metadata; refreshing table schema", columnName);
                return true;
            }
            else {
                final int localType = column.nativeType();
                final int incomingType = message.getType().getOid();
                if (localType != incomingType) {
                    LOGGER.info("detected new type for column '{}', old type was {} ({}), new type is {} ({}); refreshing table schema", columnName, localType, column.typeName(),
                                incomingType, message.getType().getName());
                    return true;
                }
                if (metadataInMessage) {
                    final int localLength = column.length();
                    final int incomingLength = message.getTypeMetadata().getLength();
                    if (localLength != incomingLength) {
                        LOGGER.info("detected new length for column '{}', old length was {}, new length is {}; refreshing table schema", columnName, localLength,
                                    incomingLength);
                        return true;
                    }
                    final int localScale = column.scale().get();
                    final int incomingScale = message.getTypeMetadata().getScale();
                    if (localScale != incomingScale) {
                        LOGGER.info("detected new scale for column '{}', old scale was {}, new scale is {}; refreshing table schema", columnName, localScale,
                                    incomingScale);
                        return true;
                    }
                    final boolean localOptional = column.isOptional();
                    final boolean incomingOptional = message.isOptional();
                    if (localOptional != incomingOptional) {
                        LOGGER.info("detected new optional status for column '{}', old value was {}, new value is {}; refreshing table schema", columnName, localOptional, incomingOptional);
                        return true;
                    }
                }
            }
            return false;
        }).findFirst().isPresent();
    }

    private boolean hasMissingUntoastedColumns(Table table, List<ReplicationMessage.Column> columns) {
        List<String> msgColumnNames = columns.stream()
                .map(ReplicationMessage.Column::getName)
                .collect(Collectors.toList());

        // Compute list of table columns not present in the replication message
        List<String> missingColumnNames = table.columns()
                .stream()
                .filter(c -> !msgColumnNames.contains(c.name()))
                .map(Column::name)
                .collect(Collectors.toList());

        List<String> toastableColumns = schema.getToastableColumnsForTableId(table.id());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("msg columns: '{}' --- missing columns: '{}' --- toastableColumns: '{}",
                    String.join(",", msgColumnNames),
                    String.join(",", missingColumnNames),
                    String.join(",", toastableColumns));
        }
        // Return `true` if we have some columns not in the replication message that are not toastable or that we do
        // not recognize
        return !toastableColumns.containsAll(missingColumnNames);
    }

    private TableSchema tableSchemaFor(TableId tableId) throws SQLException {
        if (schema.isFilteredOut(tableId)) {
            LOGGER.debug("table '{}' is filtered out, ignoring", tableId);
            return null;
        }
        TableSchema tableSchema = schema.schemaFor(tableId);
        if (tableSchema != null) {
            return tableSchema;
        }
        // we don't have a schema registered for this table, even though the filters would allow it...
        // which means that is a newly created table; so refresh our schema to get the definition for this table
        try (final PostgresConnection connection = taskContext.createConnection()) {
            schema.refresh(connection, tableId, taskContext.config().skipRefreshSchemaOnMissingToastableData());
        }
        tableSchema = schema.schemaFor(tableId);
        if (tableSchema == null) {
            LOGGER.warn("cannot load schema for table '{}'", tableId);
            return null;
        } else {
            LOGGER.debug("refreshed DB schema to include table '{}'", tableId);
            return tableSchema;
        }
    }

    private synchronized PgConnection typeResolverConnection() throws SQLException {
        return (PgConnection) connection.connection();
    }

    private Table tableFromFromMessage(List<ReplicationMessage.Column> columns, Table table) {
        final TableEditor combinedTable = table.edit()
            .setColumns(columns.stream()
                .map(column -> {
                    final PostgresType type = column.getType();
                    final ColumnEditor columnEditor = Column.editor()
                            .name(column.getName())
                            .jdbcType(type.getJdbcId())
                            .type(type.getName())
                            .optional(column.isOptional())
                            .nativeType(type.getOid());
                    columnEditor.length(column.getTypeMetadata().getLength());
                    columnEditor.scale(column.getTypeMetadata().getScale());
                    return columnEditor.create();
                })
                .collect(Collectors.toList())
            );
        final List<String> pkCandidates = table.filterColumnNames(c -> table.isPrimaryKeyColumn(c.name()));
        final Iterator<String> itPkCandidates = pkCandidates.iterator();
        while (itPkCandidates.hasNext()) {
            final String candidateName = itPkCandidates.next();
            if (!combinedTable.hasUniqueValues() && combinedTable.columnWithName(candidateName) == null) {
                LOGGER.error("Potentional inconsistency in key for message {}", columns);
                itPkCandidates.remove();
            }
        }
        combinedTable.setPrimaryKeyNames(pkCandidates);
        return combinedTable.create();
    }

    private TopicSelector<TableId> topicSelector() {
        return taskContext.topicSelector();
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        PgConnection get() throws SQLException;
    }
}

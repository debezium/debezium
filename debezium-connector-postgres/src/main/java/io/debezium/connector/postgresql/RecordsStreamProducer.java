/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.jdbc.PgConnection;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.LoggingContext;
import io.debezium.util.Strings;

/**
 * A {@link RecordsProducer} which creates {@link SourceRecord records} from a
 * Postgres streaming replication connection and {@link ReplicationMessage
 * messages}.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class RecordsStreamProducer extends RecordsProducer {

    private static final String CONTEXT_NAME = "records-stream-producer";

    private final ExecutorService executorService;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream;
    private PgConnection typeResolverConnection = null;

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        PgConnection get() throws SQLException;
    }

    /**
     * Creates new producer instance for the given task context
     *
     * @param taskContext a {@link PostgresTaskContext}, never null
     * @param sourceInfo a {@link SourceInfo} instance to track stored offsets
     */
    public RecordsStreamProducer(PostgresTaskContext taskContext,
                                 SourceInfo sourceInfo) {
        super(taskContext, sourceInfo);
        this.executorService = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, CONTEXT_NAME + "-thread"));
        this.replicationStream = new AtomicReference<>();
        try {
            this.replicationConnection = taskContext.createReplicationConnection();
        } catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected synchronized void start(Consumer<SourceRecord> recordConsumer)  {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            if (executorService.isShutdown()) {
                logger.info("Streaming will not start, stop already requested");
                return;
            }
            if (sourceInfo.hasLastKnownPosition()) {
                // start streaming from the last recorded position in the offset
                Long lsn = sourceInfo.lsn();
                if (logger.isDebugEnabled()) {
                    logger.debug("retrieved latest position from stored offset '{}'", ReplicationConnection.format(lsn));
                }
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn));
            } else {
                logger.info("no previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                replicationStream.compareAndSet(null, replicationConnection.startStreaming());
            }

            // refresh the schema so we have a latest view of the DB tables
            taskContext.refreshSchema(true);

            // the new thread will inherit it's parent MDC
            executorService.submit(() -> streamChanges(recordConsumer));
        } catch (Throwable t) {
            throw new ConnectException(t.getCause() != null ? t.getCause() : t);
        } finally {
            previousContext.restore();
        }
    }

    private void streamChanges(Consumer<SourceRecord> consumer) {
        ReplicationStream stream = this.replicationStream.get();
        // run while we haven't been requested to stop
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // this will block until a message is available
                stream.read(x -> process(x, stream.lastReceivedLSN(), consumer));
            } catch (SQLException e) {
                Throwable cause = e.getCause();
                if (cause != null && (cause instanceof IOException)) {
                    //TODO author=Horia Chiorean date=08/11/2016 description=this is because we can't safely close the stream atm
                    logger.warn("Closing replication stream due to db connection IO exception...");
                } else {
                    logger.error("unexpected exception while streaming logical changes", e);
                }
                taskContext.failTask(e);
                throw new ConnectException(e);
            } catch (Throwable e) {
                logger.error("unexpected exception while streaming logical changes", e);
                taskContext.failTask(e);
                throw new ConnectException(e);
            }
        }
    }

    @Override
    protected synchronized void commit()  {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            ReplicationStream replicationStream = this.replicationStream.get();
            if (replicationStream != null) {
                // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
                logger.debug("flushing offsets to server...");
                replicationStream.flushLSN();
            } else {
                logger.debug("streaming has already stopped, ignoring commit callback...");
            }
        } catch (SQLException e) {
            throw new ConnectException(e);
        } finally {
            previousContext.restore();
        }
    }

    @Override
    protected synchronized void stop() {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);

        try {
            if (replicationStream.get() == null) {
                logger.debug("already stopped....");
                return;
            }

            closeConnections();
        } finally {
            replicationStream.set(null);
            executorService.shutdownNow();
            previousContext.restore();
        }
    }

    private void closeConnections() {
        Exception closingException = null;

        try {
            if (replicationConnection != null) {
                logger.debug("stopping streaming...");
                //TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
                //replicationStream.close();
                // close the connection - this should also disconnect the current stream even if it's blocking
                replicationConnection.close();
            }
        }
        catch(Exception e) {
            closingException = e;
        }
        finally {
            try {
                if (typeResolverConnection != null) {
                    typeResolverConnection.close();
                }
            }
            catch(Exception e) {
                ConnectException rethrown = new ConnectException(e);
                if (closingException != null) {
                    rethrown.addSuppressed(closingException);
                }

                throw rethrown;
            }

            if (closingException != null) {
                throw new ConnectException(closingException);
            }
        }
    }

    private void process(ReplicationMessage message, Long lsn, Consumer<SourceRecord> consumer) throws SQLException {
        if (message == null) {
            // in some cases we can get null if PG gives us back a message earlier than the latest reported flushed LSN
            return;
        }

        TableId tableId = PostgresSchema.parse(message.getTable());
        assert tableId != null;

        // update the source info with the coordinates for this message
        long commitTimeNs = message.getCommitTime();
        int txId = message.getTransactionId();
        sourceInfo.update(lsn, commitTimeNs, txId);
        if (logger.isDebugEnabled()) {
            logger.debug("received new message at position {}\n{}", ReplicationConnection.format(lsn), message);
        }

        TableSchema tableSchema = tableSchemaFor(tableId);
        if (tableSchema == null) {
            return;
        }
        if (tableSchema.keySchema() == null) {
            logger.warn("ignoring message for table '{}' because it does not have a primary key defined", tableId);
        }

        ReplicationMessage.Operation operation = message.getOperation();
        switch (operation) {
            case INSERT: {
                Object[] row = columnValues(message.getNewTupleList(), tableId, true, message.hasMetadata());
                generateCreateRecord(tableId, row, consumer);
                break;
            }
            case UPDATE: {
                Object[] newRow = columnValues(message.getNewTupleList(), tableId, true, message.hasMetadata());
                Object[] oldRow = columnValues(message.getOldTupleList(), tableId, false, message.hasMetadata());
                generateUpdateRecord(tableId, oldRow, newRow, consumer);
                break;
            }
            case DELETE: {
                Object[] row = columnValues(message.getOldTupleList(), tableId, false, message.hasMetadata());
                generateDeleteRecord(tableId, row, consumer);
                break;
            }
            default: {
               logger.warn("unknown message operation: " + operation);
            }
        }
    }

    protected void generateCreateRecord(TableId tableId, Object[] rowData, Consumer<SourceRecord> recordConsumer) {
        if (rowData == null || rowData.length == 0) {
            logger.warn("no new values found for table '{}' from update message at '{}';skipping record" , tableId, sourceInfo);
            return;
        }
        TableSchema tableSchema = schema().schemaFor(tableId);
        assert tableSchema != null;
        Object key = tableSchema.keyFromColumnData(rowData);
        Struct value = tableSchema.valueFromColumnData(rowData);
        if (key == null || value == null) {
            return;
        }
        Schema keySchema = tableSchema.keySchema();
        Map<String, ?> partition = sourceInfo.partition();
        Map<String, ?> offset = sourceInfo.offset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = createEnvelope(tableSchema, topicName);

        SourceRecord record = new SourceRecord(partition, offset, topicName, null, keySchema, key, envelope.schema(),
                                               envelope.create(value, sourceInfo.source(), clock().currentTimeInMillis()));
        if (logger.isDebugEnabled()) {
            logger.debug("sending create event '{}' to topic '{}'", record, topicName);
        }
        recordConsumer.accept(record);
    }

    protected void generateUpdateRecord(TableId tableId, Object[] oldRowData, Object[] newRowData,
                                        Consumer<SourceRecord> recordConsumer) {
        if (newRowData == null || newRowData.length == 0) {
            logger.warn("no values found for table '{}' from update message at '{}';skipping record" , tableId, sourceInfo);
            return;
        }
        Schema oldKeySchema = null;
        Struct oldValue = null;
        Object oldKey = null;

        TableSchema tableSchema = schema().schemaFor(tableId);
        assert tableSchema != null;

        if (oldRowData != null && oldRowData.length > 0) {
            oldKey = tableSchema.keyFromColumnData(oldRowData);
            oldKeySchema = tableSchema.keySchema();
            oldValue = tableSchema.valueFromColumnData(oldRowData);
        }

        Object newKey = tableSchema.keyFromColumnData(newRowData);
        Struct newValue = tableSchema.valueFromColumnData(newRowData);

        Schema newKeySchema = tableSchema.keySchema();
        Map<String, ?> partition = sourceInfo.partition();
        Map<String, ?> offset = sourceInfo.offset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = createEnvelope(tableSchema, topicName);
        Struct source = sourceInfo.source();

        if (oldKey != null && !Objects.equals(oldKey, newKey)) {
            // the primary key has changed, so we need to send a DELETE followed by a CREATE

            // then send a delete event for the old key ...
            SourceRecord record = new SourceRecord(partition, offset, topicName, null, oldKeySchema, oldKey, envelope.schema(),
                                                   envelope.delete(oldValue, source, clock().currentTimeInMillis()));
            if (logger.isDebugEnabled()) {
                logger.debug("sending delete event '{}' to topic '{}'", record, topicName);
            }
            recordConsumer.accept(record);

            // send a tombstone event (null value) for the old key so it can be removed from the Kafka log eventually...
            record = new SourceRecord(partition, offset, topicName, null, oldKeySchema, oldKey, null, null);
            if (logger.isDebugEnabled()) {
                logger.debug("sending tombstone event '{}' to topic '{}'", record, topicName);
            }
            recordConsumer.accept(record);

            // then send a create event for the new key...
            record = new SourceRecord(partition, offset, topicName, null, newKeySchema, newKey, envelope.schema(),
                                      envelope.create(newValue, source, clock().currentTimeInMillis()));
            if (logger.isDebugEnabled()) {
                logger.debug("sending create event '{}' to topic '{}'", record, topicName);
            }
            recordConsumer.accept(record);
        } else {
            SourceRecord record = new SourceRecord(partition, offset, topicName, null,
                                                   newKeySchema, newKey, envelope.schema(),
                                                   envelope.update(oldValue, newValue, source, clock().currentTimeInMillis()));
            recordConsumer.accept(record);
        }
    }

    protected void generateDeleteRecord(TableId tableId, Object[] oldRowData, Consumer<SourceRecord> recordConsumer) {
        if (oldRowData == null || oldRowData.length == 0) {
            logger.warn("no values found for table '{}' from update message at '{}';skipping record" , tableId, sourceInfo);
            return;
        }
        TableSchema tableSchema = schema().schemaFor(tableId);
        assert tableSchema != null;
        Object key = tableSchema.keyFromColumnData(oldRowData);
        Struct value = tableSchema.valueFromColumnData(oldRowData);
        if (key == null || value == null) {
            return;
        }
        Schema keySchema = tableSchema.keySchema();
        Map<String, ?> partition = sourceInfo.partition();
        Map<String, ?> offset = sourceInfo.offset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = createEnvelope(tableSchema, topicName);

        // create the regular delete record
        SourceRecord record = new SourceRecord(partition, offset, topicName, null,
                                               keySchema, key, envelope.schema(),
                                               envelope.delete(value, sourceInfo.source(), clock().currentTimeInMillis()));
        if (logger.isDebugEnabled()) {
            logger.debug("sending delete event '{}' to topic '{}'", record, topicName);
        }
        recordConsumer.accept(record);

        // And send a tombstone event (null value) for the old key so it can be removed from the Kafka log eventually...
        record = new SourceRecord(partition, offset, topicName, null, keySchema, key, null, null);
        if (logger.isDebugEnabled()) {
            logger.debug("sending tombstone event '{}' to topic '{}'", record, topicName);
        }
        recordConsumer.accept(record);
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId, boolean refreshSchemaIfChanged, boolean metadataInMessage)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        Table table = schema().tableFor(tableId);
        assert table != null;

        // check if we need to refresh our local schema due to DB schema changes for this table
        if (refreshSchemaIfChanged && schemaChanged(columns, table, metadataInMessage)) {
            try (final PostgresConnection connection = taskContext.createConnection()) {
                // Refresh the schema so we get information about primary keys
                schema().refresh(connection, tableId);
                // Update the schema with metadata coming from decoder message
                if (metadataInMessage) {
                    schema().refresh(tableFromFromMessage(columns, schema().tableFor(tableId)));
                }
                table = schema().tableFor(tableId);
            }
        }

        // based on the schema columns, create the values on the same position as the columns
        List<String> columnNames = table.columnNames();
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columns.size() < columnNames.size() ? columnNames.size() : columns.size()];
        columns.forEach(message -> {
            //DBZ-298 Quoted column names will be sent like that in messages, but stored unquoted in the column names
            String columnName = Strings.unquoteIdentifierPart(message.getName());
            int position = columnNames.indexOf(columnName);
            assert position >= 0;
            values[position] = message.getValue(this::typeResolverConnection, taskContext.config().includeUnknownDatatypes());
        });
        return values;
    }

    private boolean schemaChanged(List<ReplicationMessage.Column> columns, Table table, boolean metadataInMessage) {
        List<String> columnNames = table.columnNames();
        int messagesCount = columns.size();
        if (columnNames.size() != messagesCount) {
            // the table metadata has less or more columns than the event, which means the table structure has changed,
            // so we need to trigger a refresh...
           return true;
        }

        // go through the list of columns from the message to figure out if any of them are new or have changed their type based
        // on what we have in the table metadata....
        return columns.stream().filter(message -> {
            String columnName = message.getName();
            Column column = table.columnWithName(columnName);
            if (column == null) {
                logger.debug("found new column '{}' present in the server message which is not part of the table metadata; refreshing table schema", columnName);
                return true;
            } else {
                final int localType = metadataInMessage ? column.jdbcType() : PgOid.typeNameToOid(column.typeName());
                final int incomingType = metadataInMessage ? typeNameToJdbcType(message.getTypeMetadata()) : message.getOidType();
                if (localType != incomingType) {
                    logger.debug("detected new type for column '{}', old type was '{}', new type is '{}'; refreshing table schema", columnName, localType,
                                incomingType);
                    return true;
                }
            }
            return false;
        }).findFirst().isPresent();
    }

    private TableSchema tableSchemaFor(TableId tableId) throws SQLException {
        PostgresSchema schema = schema();
        if (schema.isFilteredOut(tableId)) {
            logger.debug("table '{}' is filtered out, ignoring", tableId);
            return null;
        }
        TableSchema tableSchema = schema.schemaFor(tableId);
        if (tableSchema != null) {
            return tableSchema;
        }
        // we don't have a schema registered for this table, even though the filters would allow it...
        // which means that is a newly created table; so refresh our schema to get the definition for this table
        try (final PostgresConnection connection = taskContext.createConnection()) {
            schema.refresh(connection, tableId);
        }
        tableSchema = schema.schemaFor(tableId);
        if (tableSchema == null) {
            logger.warn("cannot load schema for table '{}'", tableId);
            return null;
        } else {
            logger.debug("refreshed DB schema to include table '{}'", tableId);
            return tableSchema;
        }
    }

    private synchronized PgConnection typeResolverConnection() throws SQLException {
        if (typeResolverConnection == null) {
            typeResolverConnection = (PgConnection)taskContext.createConnection().connection();
        }
        return typeResolverConnection;
    }

    private Table tableFromFromMessage(List<ReplicationMessage.Column> columns, Table table) {
        return table.edit()
            .setColumns(columns.stream()
                .map(column -> {
                    final ColumnEditor columnEditor = Column.editor()
                            .name(column.getName())
                            .jdbcType(column.getOidType() == Types.ARRAY ? Types.ARRAY : typeNameToJdbcType(column.getTypeMetadata()))
                            .type(column.getTypeMetadata().getName())
                            .optional(column.isOptional());
                    PgOid.reconcileJdbcOidTypeConstraints(column.getTypeMetadata(), columnEditor);
                    if (column.getOidType() == Types.ARRAY) {
                        columnEditor.componentType(column.getComponentOidType());
                    }
                    if (column.getTypeMetadata().getLength().isPresent()) {
                        columnEditor.length(column.getTypeMetadata().getLength().getAsInt());
                    }
                    if (column.getTypeMetadata().getScale().isPresent()) {
                        columnEditor.scale(column.getTypeMetadata().getScale().getAsInt());
                    }
                    return columnEditor.create();
                })
                .collect(Collectors.toList())
            )
            .setPrimaryKeyNames(table.filterColumnNames(c -> table.isPrimaryKeyColumn(c.name()))).create();
    }

    private int typeNameToJdbcType(final ReplicationMessage.ColumnTypeMetadata columnTypeMetadata) {
        return taskContext.schema().columnTypeNameToJdbcTypeId(columnTypeMetadata.getName());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgConnection;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.proto.PgProto;
import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.LoggingContext;
import io.debezium.util.Strings;

/**
 * A {@link RecordsProducer} which creates {@link org.apache.kafka.connect.source.SourceRecord records} from a Postgres
 * streaming replication connection and {@link io.debezium.connector.postgresql.proto.PgProto messages}.
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
    protected void start(Consumer<SourceRecord> recordConsumer)  {
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
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
                PgProto.RowMessage message = stream.read();
                process(message, stream.lastReceivedLSN(), consumer);
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
            if (replicationConnection != null) {
                logger.debug("stopping streaming...");
                //TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
                //replicationStream.close();
                // close the connection - this should also disconnect the current stream even if it's blocking
                replicationConnection.close();
            }
        } catch (Exception e) {
            throw new ConnectException(e.getCause() != null ? e.getCause() : e);
        } finally {
            replicationStream.set(null);
            executorService.shutdownNow();
            previousContext.restore();
        }
    }

    private void process(PgProto.RowMessage message, Long lsn, Consumer<SourceRecord> consumer) throws SQLException {
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

        PgProto.Op operation = message.getOp();
        switch (operation) {
            case INSERT: {
                Object[] row = columnValues(message.getNewTupleList(), tableId, true);
                generateCreateRecord(tableId, row, consumer);
                break;
            }
            case UPDATE: {
                Object[] oldRow = columnValues(message.getOldTupleList(), tableId, true);
                Object[] newRow = columnValues(message.getNewTupleList(), tableId, true);
                generateUpdateRecord(tableId, oldRow, newRow, consumer);
                break;
            }
            case DELETE: {
                Object[] row = columnValues(message.getOldTupleList(), tableId, false);
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

    private Object[] columnValues(List<PgProto.DatumMessage> messageList, TableId tableId, boolean refreshSchemaIfChanged)
            throws SQLException {
        if (messageList == null || messageList.isEmpty()) {
            return null;
        }
        Table table = schema().tableFor(tableId);
        assert table != null;

        // check if we need to refresh our local schema due to DB schema changes for this table
        if (refreshSchemaIfChanged && schemaChanged(messageList, table)) {
            schema().refresh(taskContext.createConnection(), tableId);
            table = schema().tableFor(tableId);
        }

        // based on the schema columns, create the values on the same position as the columns
        List<String> columnNames = table.columnNames();
        Object[] values = new Object[messageList.size()];
        messageList.forEach(message -> {
            //DBZ-298 Quoted column names will be sent like that in messages, but stored unquoted in the column names
            String columnName = Strings.unquoteIdentifierPart(message.getColumnName());
            int position = columnNames.indexOf(columnName);
            assert position >= 0;
            values[position] = extractValueFromMessage(message);
        });
        return values;
    }

    private boolean schemaChanged(List<PgProto.DatumMessage> messageList, Table table) {
        List<String> columnNames = table.columnNames();
        int messagesCount = messageList.size();
        if (columnNames.size() != messagesCount) {
            // the table metadata has less or more columns than the event, which means the table structure has changed,
            // so we need to trigger a refresh...
           return true;
        }

        // go through the list of columns from the message to figure out if any of them are new or have changed their type based
        // on what we have in the table metadata....
        return messageList.stream().filter(message -> {
            String columnName = message.getColumnName();
            Column column = table.columnWithName(columnName);
            if (column == null) {
                logger.debug("found new column '{}' present in the server message which is not part of the table metadata; refreshing table schema", columnName);
                return true;
            } else if (!schema().isType(column.typeName(), column.jdbcType())) {
                logger.debug("detected new type for column '{}', old type was '{}', new type is '{}'; refreshing table schema", columnName, column.jdbcType(),
                            message.getColumnType());
                return true;
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
        schema.refresh(taskContext.createConnection(), tableId);
        tableSchema = schema.schemaFor(tableId);
        if (tableSchema == null) {
            logger.warn("cannot load schema for table '{}'", tableId);
            return null;
        } else {
            logger.debug("refreshed DB schema to include table '{}'", tableId);
            return tableSchema;
        }
    }

    /**
     * Converts the Protobuf value for a {@link io.debezium.connector.postgresql.proto.PgProto.DatumMessage plugin message} to
     * a Java value based on the type of the column from the message. This value will be converted later on if necessary by the
     * {@link PostgresValueConverter#converter(Column, Field)} instance to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled (i.e. dependent) on the Postgres plugin logic which writes the actual
     * Protobuf messages.
     *
     * @param datumMessage  a {@link io.debezium.connector.postgresql.proto.PgProto.DatumMessage} instance; never {@code null}
     * @return the value; may be null
     */
    protected Object extractValueFromMessage(PgProto.DatumMessage datumMessage) {
        int columnType = (int) datumMessage.getColumnType();
        switch (columnType) {
            case PgOid.BOOL:
                return datumMessage.hasDatumBool() ? datumMessage.getDatumBool() : null;
            case PgOid.INT2:
            case PgOid.INT4:
                return datumMessage.hasDatumInt32() ? datumMessage.getDatumInt32() : null;
            case PgOid.INT8:
            case PgOid.OID:
            case PgOid.MONEY:
                return datumMessage.hasDatumInt64() ? datumMessage.getDatumInt64() : null;
            case PgOid.FLOAT4:
                return datumMessage.hasDatumFloat()? datumMessage.getDatumFloat() : null;
            case PgOid.FLOAT8:
            case PgOid.NUMERIC:
                return datumMessage.hasDatumDouble() ? datumMessage.getDatumDouble() : null;
            case PgOid.CHAR:
            case PgOid.VARCHAR:
            case PgOid.BPCHAR:
            case PgOid.TEXT:
            case PgOid.JSON:
            case PgOid.JSONB_OID:
            case PgOid.XML:
            case PgOid.UUID:
            case PgOid.BIT:
            case PgOid.VARBIT:
                return datumMessage.hasDatumString() ? datumMessage.getDatumString() : null;
            case PgOid.DATE:
                return datumMessage.hasDatumInt32() ? (long) datumMessage.getDatumInt32() : null;
            case PgOid.TIMESTAMP:
            case PgOid.TIMESTAMPTZ:
            case PgOid.TIME:
                if (!datumMessage.hasDatumInt64()) {
                    return null;
                }
                // these types are sent by the plugin as LONG - microseconds since Unix Epoch
                // but we'll convert them to nanos which is the smallest unit
                return TimeUnit.NANOSECONDS.convert(datumMessage.getDatumInt64(), TimeUnit.MICROSECONDS);
            case PgOid.TIMETZ:
                if (!datumMessage.hasDatumDouble()) {
                    return null;
                }
                // the value is sent as a double microseconds, convert to nano
                return BigDecimal.valueOf(datumMessage.getDatumDouble() * 1000).longValue();
            case PgOid.INTERVAL:
                // these are sent as doubles by the plugin since their storage is larger than 8 bytes
                return datumMessage.hasDatumDouble() ? datumMessage.getDatumDouble() : null;
            // the plugin will send back a TZ formatted string
            case PgOid.BYTEA:
                return datumMessage.hasDatumBytes() ? datumMessage.getDatumBytes().toByteArray() : null;
            case PgOid.POINT: {
                PgProto.Point datumPoint = datumMessage.getDatumPoint();
                return new PGpoint(datumPoint.getX(), datumPoint.getY());
            }
            case PgOid.TSTZRANGE_OID:
                return datumMessage.hasDatumBytes() ? new String(datumMessage.getDatumBytes().toByteArray(), Charset.forName("UTF-8")) : null;
            case PgOid.INT2_ARRAY:
            case PgOid.INT4_ARRAY:
            case PgOid.INT8_ARRAY:
            case PgOid.TEXT_ARRAY:
            case PgOid.NUMERIC_ARRAY:
            case PgOid.FLOAT4_ARRAY:
            case PgOid.FLOAT8_ARRAY:
            case PgOid.BOOL_ARRAY:
            case PgOid.DATE_ARRAY:
            case PgOid.TIME_ARRAY:
            case PgOid.TIMETZ_ARRAY:
            case PgOid.TIMESTAMP_ARRAY:
            case PgOid.TIMESTAMPTZ_ARRAY:
            case PgOid.BYTEA_ARRAY:
            case PgOid.VARCHAR_ARRAY:
            case PgOid.OID_ARRAY:
            case PgOid.BPCHAR_ARRAY:
            case PgOid.MONEY_ARRAY:
            case PgOid.NAME_ARRAY:
            case PgOid.INTERVAL_ARRAY:
            case PgOid.CHAR_ARRAY:
            case PgOid.VARBIT_ARRAY:
            case PgOid.UUID_ARRAY:
            case PgOid.XML_ARRAY:
            case PgOid.POINT_ARRAY:
            case PgOid.JSONB_ARRAY:
            case PgOid.JSON_ARRAY:
            case PgOid.REF_CURSOR_ARRAY:
                // Currently the logical decoding plugin sends unhandled types as a byte array containing the string
                // representation (in Postgres) of the array value.
                // The approach to decode this is sub-optimal but the only way to improve this is to update the plugin.
                // Reasons for it being sub-optimal include:
                // 1. It requires a Postgres JDBC connection to deserialize
                // 2. The byte-array is a serialised string but we make the assumption its UTF-8 encoded (which it will
                //    be in most cases)
                // 3. For larger arrays and especially 64-bit integers and the like it is less efficient sending string
                //    representations over the wire.
                try {
                    byte[] data = datumMessage.hasDatumBytes()? datumMessage.getDatumBytes().toByteArray() : null;
                    if (data == null) return null;
                    String dataString = new String(data, Charset.forName("UTF-8"));
                    PgArray arrayData = new PgArray(typeResolverConnection(), columnType, dataString);
                    Object deserializedArray = arrayData.getArray();
                    return Arrays.asList((Object[])deserializedArray);
                }
                catch (SQLException e) {
                    logger.warn("Unexpected exception trying to process PgArray column '{}'", datumMessage.getColumnName(), e);
                }
                return null;
            default: {
                logger.warn("processing column '{}' with unknown data type '{}' as byte array", datumMessage.getColumnName(),
                            datumMessage.getColumnType());
                return datumMessage.hasDatumBytes()? datumMessage.getDatumBytes().toByteArray() : null;
            }
        }
    }


    private synchronized PgConnection typeResolverConnection() throws SQLException {
        if (typeResolverConnection == null) {
            typeResolverConnection = (PgConnection)taskContext.createConnection().connection();
        }
        return typeResolverConnection;
    }
}

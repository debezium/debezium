/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.HistoryRecord.Fields;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * A component that makes {@link SourceRecord}s for tables.
 *
 * @author Randall Hauch
 */
public class RecordMakers {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MySqlSchema schema;
    private final SourceInfo source;
    private final TopicSelector<TableId> topicSelector;
    private final boolean emitTombstoneOnDelete;
    private final Map<Long, Converter> convertersByTableNumber = new HashMap<>();
    private final Map<TableId, Long> tableNumbersByTableId = new HashMap<>();
    private final Map<Long, TableId> tableIdsByTableNumber = new HashMap<>();
    private final Schema schemaChangeKeySchema;
    private final Schema schemaChangeValueSchema;
    private final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(logger);
    private Map<String, ?> restartOffset = null;

    /**
     * Create the record makers using the supplied components.
     *
     * @param schema the schema information about the MySQL server databases; may not be null
     * @param source the connector's source information; may not be null
     * @param topicSelector the selector for topic names; may not be null
     */
    public RecordMakers(MySqlSchema schema, SourceInfo source, TopicSelector<TableId> topicSelector, boolean emitTombstoneOnDelete) {
        this.schema = schema;
        this.source = source;
        this.topicSelector = topicSelector;
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
        this.schemaChangeKeySchema = SchemaBuilder.struct()
                                                  .name(schemaNameAdjuster.adjust("io.debezium.connector.mysql.SchemaChangeKey"))
                                                  .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                                                  .build();
        this.schemaChangeValueSchema = SchemaBuilder.struct()
                                                    .name(schemaNameAdjuster.adjust("io.debezium.connector.mysql.SchemaChangeValue"))
                                                    .field(Fields.SOURCE, SourceInfo.SCHEMA)
                                                    .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                                                    .field(Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA)
                                                    .build();
    }

    /**
     * @param restartOffset the offset to publish with the {@link SourceInfo#RESTART_PREFIX} prefix
     *                      as additional information in the offset. If the connector attempts to
     *                      restart from an offset with information with this prefix it will
     *                      create an offset from the prefixed information rather than restarting
     *                      from the base offset.
     * @see RecordMakers#RecordMakers(MySqlSchema, SourceInfo, TopicSelector, boolean)
     * @see MySqlConnectorTask#getRestartOffset(Map)
     */
    public RecordMakers(MySqlSchema schema,
                        SourceInfo source,
                        TopicSelector<TableId> topicSelector,
                        boolean emitTombstoneOnDelete,
                        Map<String, ?> restartOffset) {
        this(schema, source, topicSelector, emitTombstoneOnDelete);
        this.restartOffset = restartOffset;
    }

    /**
     * Obtain the record maker for the given table, using the specified columns and sending records to the given consumer.
     *
     * @param tableId the identifier of the table for which records are to be produced
     * @param includedColumns the set of columns that will be included in each row; may be null if all columns are included
     * @param consumer the consumer for all produced records; may not be null
     * @return the table-specific record maker; may be null if the table is not included in the connector
     */
    public RecordsForTable forTable(TableId tableId, BitSet includedColumns, BlockingConsumer<SourceRecord> consumer) {
        Long tableNumber = tableNumbersByTableId.get(tableId);
        return tableNumber != null ? forTable(tableNumber, includedColumns, consumer) : null;
    }

    /**
     * Determine if there is a record maker for the given table.
     *
     * @param tableId the identifier of the table
     * @return {@code true} if there is a {@link #forTable(TableId, BitSet, BlockingConsumer) record maker}, or {@code false}
     * if there is none
     */
    public boolean hasTable(TableId tableId) {
        Long tableNumber = tableNumbersByTableId.get(tableId);
        if ( tableNumber == null ) return false;
        Converter converter = convertersByTableNumber.get(tableNumber);
        return converter != null;
    }

    /**
     * Obtain the record maker for the given table, using the specified columns and sending records to the given consumer.
     *
     * @param tableNumber the {@link #assign(long, TableId) assigned table number} for which records are to be produced
     * @param includedColumns the set of columns that will be included in each row; may be null if all columns are included
     * @param consumer the consumer for all produced records; may not be null
     * @return the table-specific record maker; may be null if the table is not included in the connector
     */
    public RecordsForTable forTable(long tableNumber, BitSet includedColumns, BlockingConsumer<SourceRecord> consumer) {
        Converter converter = convertersByTableNumber.get(tableNumber);
        if (converter == null) return null;
        return new RecordsForTable(converter, includedColumns, consumer);
    }

    /**
     * Produce a schema change record for the given DDL statements.
     *
     * @param databaseName the name of the database that is affected by the DDL statements; may not be null
     * @param ddlStatements the DDL statements; may not be null
     * @param consumer the consumer for all produced records; may not be null
     * @return the number of records produced; will be 0 or more
     */
    public int schemaChanges(String databaseName, String ddlStatements, BlockingConsumer<SourceRecord> consumer) {
        String topicName = topicSelector.getPrimaryTopic();
        Integer partition = 0;
        Struct key = schemaChangeRecordKey(databaseName);
        Struct value = schemaChangeRecordValue(databaseName, ddlStatements);
        SourceRecord record = new SourceRecord(source.partition(), source.offset(),
                topicName, partition, schemaChangeKeySchema, key, schemaChangeValueSchema, value);
        try {
            consumer.accept(record);
            return 1;
        } catch (InterruptedException e) {
            return 0;
        }
    }

    /**
     * Clear all of the cached record makers. This should be done when the logs are rotated, since in that a different table
     * numbering scheme will be used by all subsequent TABLE_MAP binlog events.
     */
    public void clear() {
        logger.debug("Clearing table converters");
        convertersByTableNumber.clear();
        tableNumbersByTableId.clear();
        tableIdsByTableNumber.clear();
    }

    /**
     * Clear all of the cached record makers and generate new ones. This should be done when the schema changes for reasons other
     * than reading DDL from the binlog.
     */
    public void regenerate() {
        clear();
        AtomicInteger nextTableNumber = new AtomicInteger(0);
        Set<TableId> tableIds = schema.tableIds();
        logger.debug("Regenerating converters for {} tables", tableIds.size());
        tableIds.forEach(id -> {
            assign(nextTableNumber.incrementAndGet(), id);
        });
    }

    private Map<String, ?> getSourceRecordOffset(Map<String, ?> sourceOffset) {
        if (restartOffset == null) {
            return sourceOffset;
        }
        else {
            Map<String, Object> offset = (Map<String, Object>) sourceOffset;
            for(String key : restartOffset.keySet()){
                StringBuilder sb = new StringBuilder(SourceInfo.RESTART_PREFIX);
                offset.put(sb.append(key).toString(), restartOffset.get(key));
            }
            return offset;
        }
    }

    /**
     * Assign the given table number to the table with the specified {@link TableId table ID}.
     *
     * @param tableNumber the table number found in binlog events
     * @param id the identifier for the corresponding table
     * @return {@code true} if the assignment was successful, or {@code false} if the table is currently excluded in the
     *         connector's configuration
     */
    public boolean assign(long tableNumber, TableId id) {
        Long existingTableNumber = tableNumbersByTableId.get(id);
        if (existingTableNumber != null && existingTableNumber.longValue() == tableNumber
                && convertersByTableNumber.containsKey(tableNumber)) {
            // This is the exact same table number for the same table, so do nothing ...
            return true;
        }
        TableSchema tableSchema = schema.schemaFor(id);
        if (tableSchema == null) return false;

        String topicName = topicSelector.topicNameFor(id);
        Envelope envelope = tableSchema.getEnvelopeSchema();

        // Generate this table's insert, update, and delete converters ...
        Integer partitionNum = null;
        Converter converter = new Converter() {

            @Override
            public int read(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                            BlockingConsumer<SourceRecord> consumer)
                    throws InterruptedException {
                Object key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if (value != null || key != null) {
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, ?> offset = source.offsetForRow(rowNumber, numberOfRows);
                    Struct origin = source.struct(id);
                    SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                            keySchema, key, envelope.schema(), envelope.read(value, origin, ts));
                    consumer.accept(record);
                    return 1;
                }
                return 0;
            }

            @Override
            public int insert(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                              BlockingConsumer<SourceRecord> consumer)
                    throws InterruptedException {
                Object key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if (value != null || key != null) {
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, ?> offset = source.offsetForRow(rowNumber, numberOfRows);
                    Struct origin = source.struct(id);
                    SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                            keySchema, key, envelope.schema(), envelope.create(value, origin, ts));
                    consumer.accept(record);
                    return 1;
                }
                return 0;
            }

            @Override
            public int update(SourceInfo source, Object[] before, Object[] after, int rowNumber, int numberOfRows, BitSet includedColumns,
                              long ts,
                              BlockingConsumer<SourceRecord> consumer)
                    throws InterruptedException {
                int count = 0;
                Object key = tableSchema.keyFromColumnData(after);
                Struct valueAfter = tableSchema.valueFromColumnData(after);
                if (valueAfter != null || key != null) {
                    Object oldKey = tableSchema.keyFromColumnData(before);
                    Struct valueBefore = tableSchema.valueFromColumnData(before);
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, ?> offset = source.offsetForRow(rowNumber, numberOfRows);
                    Struct origin = source.struct(id);
                    if (key != null && !Objects.equals(key, oldKey)) {
                        // The key has changed, so we need to deal with both the new key and old key.
                        // Consumers may push the events into a system that won't allow both records to exist at the same time,
                        // so we first want to send the delete event for the old key...
                        SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                                keySchema, oldKey, envelope.schema(), envelope.delete(valueBefore, origin, ts));
                        consumer.accept(record);
                        ++count;

                        if (emitTombstoneOnDelete) {
                            // Next send a tombstone event for the old key ...
                            record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum, keySchema, oldKey, null, null);
                            consumer.accept(record);
                            ++count;
                        }

                        // And finally send the create event ...
                        record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                                keySchema, key, envelope.schema(), envelope.create(valueAfter, origin, ts));
                        consumer.accept(record);
                        ++count;
                    } else {
                        // The key has not changed, so a simple update is fine ...
                        SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                                keySchema, key, envelope.schema(), envelope.update(valueBefore, valueAfter, origin, ts));
                        consumer.accept(record);
                        ++count;
                    }
                }
                return count;
            }

            @Override
            public int delete(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                              BlockingConsumer<SourceRecord> consumer)
                    throws InterruptedException {
                int count = 0;
                Object key = tableSchema.keyFromColumnData(row);
                Struct value = tableSchema.valueFromColumnData(row);
                if (value != null || key != null) {
                    Schema keySchema = tableSchema.keySchema();
                    Map<String, ?> partition = source.partition();
                    Map<String, ?> offset = source.offsetForRow(rowNumber, numberOfRows);
                    Struct origin = source.struct(id);
                    // Send a delete message ...
                    SourceRecord record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                            keySchema, key, envelope.schema(), envelope.delete(value, origin, ts));
                    consumer.accept(record);
                    ++count;

                    // And send a tombstone ...
                    if (emitTombstoneOnDelete) {
                        record = new SourceRecord(partition, getSourceRecordOffset(offset), topicName, partitionNum,
                                keySchema, key, null, null);
                        consumer.accept(record);
                        ++count;
                    }
                }
                return count;
            }

            @Override
            public String toString() {
                return "RecordMaker.Converter(" + id + ")";
            }

        };

        convertersByTableNumber.put(tableNumber, converter);
        Long previousTableNumber = tableNumbersByTableId.put(id, tableNumber);
        tableIdsByTableNumber.put(tableNumber, id);
        if (previousTableNumber != null) {
            assert previousTableNumber.longValue() != tableNumber;
            convertersByTableNumber.remove(previousTableNumber);
        }
        return true;
    }

    protected Struct schemaChangeRecordKey(String databaseName) {
        Struct result = new Struct(schemaChangeKeySchema);
        result.put(Fields.DATABASE_NAME, databaseName);
        return result;
    }

    protected Struct schemaChangeRecordValue(String databaseName, String ddlStatements) {
        Struct result = new Struct(schemaChangeValueSchema);
        result.put(Fields.SOURCE, source.struct());
        result.put(Fields.DATABASE_NAME, databaseName);
        result.put(Fields.DDL_STATEMENTS, ddlStatements);
        return result;
    }

    protected static interface Converter {
        int read(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                 BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException;

        int insert(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                   BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException;

        int update(SourceInfo source, Object[] before, Object[] after, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                   BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException;

        int delete(SourceInfo source, Object[] row, int rowNumber, int numberOfRows, BitSet includedColumns, long ts,
                   BlockingConsumer<SourceRecord> consumer)
                throws InterruptedException;

    }

    /**
     * A {@link SourceRecord} factory for a specific table and consumer.
     */
    public final class RecordsForTable {
        private final BitSet includedColumns;
        private final Converter converter;
        private final BlockingConsumer<SourceRecord> consumer;

        protected RecordsForTable(Converter converter, BitSet includedColumns, BlockingConsumer<SourceRecord> consumer) {
            this.converter = converter;
            this.includedColumns = includedColumns;
            this.consumer = consumer;
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#READ read} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int read(Object[] row, long ts) throws InterruptedException {
            return read(row, ts, 0, 1);
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#READ read} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @param rowNumber the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int read(Object[] row, long ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.read(source, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#CREATE create} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int create(Object[] row, long ts) throws InterruptedException {
            return create(row, ts, 0, 1);
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#CREATE create} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @param rowNumber the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int create(Object[] row, long ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.insert(source, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce an {@link io.debezium.data.Envelope.Operation#UPDATE update} record for the row.
         *
         * @param before the values of the row <i>before</i> the update, in the same order as the columns in the {@link Table}
         *            definition in the {@link MySqlSchema}
         * @param after the values of the row <i>after</i> the update, in the same order as the columns in the {@link Table}
         *            definition in the {@link MySqlSchema}
         * @param ts the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int update(Object[] before, Object[] after, long ts) throws InterruptedException {
            return update(before, after, ts, 0, 1);
        }

        /**
         * Produce an {@link io.debezium.data.Envelope.Operation#UPDATE update} record for the row.
         *
         * @param before the values of the row <i>before</i> the update, in the same order as the columns in the {@link Table}
         *            definition in the {@link MySqlSchema}
         * @param after the values of the row <i>after</i> the update, in the same order as the columns in the {@link Table}
         *            definition in the {@link MySqlSchema}
         * @param ts the timestamp for this row
         * @param rowNumber the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int update(Object[] before, Object[] after, long ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.update(source, before, after, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#DELETE delete} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int delete(Object[] row, long ts) throws InterruptedException {
            return delete(row, ts, 0, 1);
        }

        /**
         * Produce a {@link io.debezium.data.Envelope.Operation#DELETE delete} record for the row.
         *
         * @param row the values of the row, in the same order as the columns in the {@link Table} definition in the
         *            {@link MySqlSchema}.
         * @param ts the timestamp for this row
         * @param rowNumber the number of this row; must be 0 or more
         * @param numberOfRows the total number of rows to be read; must be 1 or more
         * @return the number of records produced; will be 0 or more
         * @throws InterruptedException if this thread is interrupted while waiting to give a source record to the consumer
         */
        public int delete(Object[] row, long ts, int rowNumber, int numberOfRows) throws InterruptedException {
            return converter.delete(source, row, rowNumber, numberOfRows, includedColumns, ts, consumer);
        }
    }

    /**
     * Converts table number back to table id
     *
     * @param tableNumber
     * @return the table id or null for unknown tables
     */
    public TableId getTableIdFromTableNumber(long tableNumber) {
        return tableIdsByTableNumber.get(tableNumber);
    }
}

/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.data.Envelope;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.HistoryRecord.Fields;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.text.ParsingException;
import io.debezium.util.Clock;
import io.debezium.util.Collect;

/**
 * @author Randall Hauch
 *
 */
@NotThreadSafe
final class TableConverters {

    protected static final Schema SCHEMA_CHANGE_RECORD_KEY_SCHEMA = SchemaBuilder.struct()
                                                                                 .name("io.debezium.connector.mysql.SchemaRecordKey")
                                                                                 .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                                                                                 .build();

    protected static final Schema SCHEMA_CHANGE_RECORD_VALUE_SCHEMA = SchemaBuilder.struct()
                                                                                   .name("io.debezium.connector.mysql.SchemaRecordKey")
                                                                                   .field(Fields.SOURCE, SourceInfo.SCHEMA)
                                                                                   .field(Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                                                                                   .field(Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA)
                                                                                   .build();

    public Struct schemaChangeRecordKey(String databaseName) {
        Struct result = new Struct(SCHEMA_CHANGE_RECORD_KEY_SCHEMA);
        result.put(Fields.DATABASE_NAME, databaseName);
        return result;
    }

    public Struct schemaChangeRecordValue(SourceInfo source, String databaseName, String ddlStatements) {
        Struct result = new Struct(SCHEMA_CHANGE_RECORD_VALUE_SCHEMA);
        result.put(Fields.SOURCE, source.struct());
        result.put(Fields.DATABASE_NAME, databaseName);
        result.put(Fields.DDL_STATEMENTS, ddlStatements);
        return result;
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final DatabaseHistory dbHistory;
    private final TopicSelector topicSelector;
    private final MySqlDdlParser ddlParser;
    private final DdlChanges ddlChanges;
    private final Tables tables;
    private final TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
    private final Map<TableId, TableSchema> tableSchemaByTableId = new HashMap<>();
    private final Map<Long, Converter> convertersByTableId = new HashMap<>();
    private final Map<String, Long> tableNumbersByTableName = new HashMap<>();
    private final boolean recordSchemaChangesInSourceRecords;
    private final Predicate<String> dbFilter;
    private final Predicate<TableId> tableFilter;
    private final Predicate<ColumnId> columnFilter;
    private final ColumnMappers columnMappers;
    private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    private final Set<TableId> unknownTableIds = new HashSet<>();
    private final Clock clock;

    public TableConverters(TopicSelector topicSelector, DatabaseHistory dbHistory,
            boolean recordSchemaChangesInSourceRecords, Clock clock, Predicate<String> dbFilter, Tables tables,
            Predicate<TableId> tableFilter, Predicate<ColumnId> columnFilter, ColumnMappers columnSelectors) {
        Objects.requireNonNull(topicSelector, "A topic selector is required");
        Objects.requireNonNull(dbHistory, "Database history storage is required");
        Objects.requireNonNull(tables, "A Tables object is required");
        Objects.requireNonNull(clock, "A Clock object is required");
        Objects.requireNonNull(dbFilter, "A database filter object is required");
        this.topicSelector = topicSelector;
        this.dbHistory = dbHistory;
        this.clock = clock;
        this.dbFilter = dbFilter;
        this.tables = tables;
        this.columnFilter = columnFilter;
        this.columnMappers = columnSelectors;
        this.ddlParser = new MySqlDdlParser(false); // don't include views
        this.ddlChanges = new DdlChanges(this.ddlParser.terminator());
        this.ddlParser.addListener(ddlChanges);
        this.recordSchemaChangesInSourceRecords = recordSchemaChangesInSourceRecords;
        Predicate<TableId> knownTables = (id) -> !unknownTableIds.contains(id); // known if not unknown
        this.tableFilter = tableFilter != null ? tableFilter.and(knownTables) : knownTables;
    }

    public void loadTables() {
        // Create TableSchema instances for any existing table ...
        this.tables.tableIds().forEach(id -> {
            Table table = this.tables.forTable(id);
            TableSchema schema = schemaBuilder.create(table, columnFilter, columnMappers);
            tableSchemaByTableId.put(id, schema);
        });
    }

    public void rotateLogs(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        logger.debug("Rotating logs: {}", event);
        RotateEventData command = event.getData();
        if (command != null) {
            // The logs are being rotated, which means the server was either restarted, or the binlog has transitioned to a new
            // file. In either case, the table numbers will change, so we need to discard the cache of converters by the table IDs
            // (e.g., the Map<Long,Converter>). Note, however, that we're NOT clearing out the Map<TableId,TableSchema>.
            convertersByTableId.clear();
        }
    }

    public void updateTableCommand(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        QueryEventData command = event.getData();
        // The command's database is the one that the client was using when submitting the DDL statements,
        // and that might not be the database(s) affected by the DDL statements ...
        String databaseName = command.getDatabase();
        String ddlStatements = command.getSql();
        if (ignoredQueryStatements.contains(ddlStatements)) return;
        logger.debug("Received update table command: {}", event);
        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, tables);
        } catch (ParsingException e) {
            logger.error("Error parsing DDL statement and updating tables: {}", ddlStatements, e);
        } finally {
            if (recordSchemaChangesInSourceRecords) {

                // We are supposed to _also_ record the schema changes as SourceRecords, but these need to be filtered
                // by database. Unfortunately, the databaseName on the event might not be the same database as that
                // being modified by the DDL statements (since the DDL statements can have fully-qualified names).
                // Therefore, we have to look at each statement to figure out which database it applies and then
                // record the DDL statements (still in the same order) to those databases.
                
                if ( !ddlChanges.isEmpty() && ddlChanges.applyToMoreDatabasesThan(databaseName) ) {
                    
                    // We understood at least some of the DDL statements and can figure out to which database they apply.
                    // They also apply to more databases than 'databaseName', so we need to apply the DDL statements in
                    // the same order they were read for each _affected_ database, grouped together if multiple apply
                    // to the same _affected_ database...
                    ddlChanges.groupStatementStringsByDatabase((dbName, statements) -> {
                        if (dbFilter.test(dbName)) {
                            String serverName = source.serverName();
                            String topicName = topicSelector.getTopic(serverName);
                            Integer partition = 0;
                            Struct key = schemaChangeRecordKey(databaseName);
                            Struct value = schemaChangeRecordValue(source, dbName, statements);
                            SourceRecord record = new SourceRecord(source.partition(), source.offset(),
                                    topicName, partition,
                                    SCHEMA_CHANGE_RECORD_KEY_SCHEMA, key,
                                    SCHEMA_CHANGE_RECORD_VALUE_SCHEMA, value);
                            recorder.accept(record);
                        }
                    });
                } else if (dbFilter.test(databaseName)) {
                    // Either all of the statements applied to 'databaseName', or we didn't understand any of the statements.
                    // But the database filter includes 'databaseName' so we should forward all of the statements ...
                    String serverName = source.serverName();
                    String topicName = topicSelector.getTopic(serverName);
                    Integer partition = 0;
                    Struct key = schemaChangeRecordKey(databaseName);
                    Struct value = schemaChangeRecordValue(source, databaseName, ddlStatements);
                    SourceRecord record = new SourceRecord(source.partition(), source.offset(),
                            topicName, partition,
                            SCHEMA_CHANGE_RECORD_KEY_SCHEMA, key,
                            SCHEMA_CHANGE_RECORD_VALUE_SCHEMA, value);
                    recorder.accept(record);
                }
            }

            // Record the DDL statement so that we can later recover them if needed. We do this _after_ writing the
            // schema change records so that failure recovery (which is based on of the history) won't lose
            // schema change records.
            dbHistory.record(source.partition(), source.offset(), databaseName, tables, ddlStatements);
        }

        // Figure out what changed ...
        Set<TableId> changes = tables.drainChanges();
        changes.forEach(tableId -> {
            Table table = tables.forTable(tableId);
            if (table == null) { // removed
                tableSchemaByTableId.remove(tableId);
            } else {
                TableSchema schema = schemaBuilder.create(table, columnFilter, columnMappers);
                tableSchemaByTableId.put(tableId, schema);
            }
        });
    }

    /**
     * Handle a change in the table metadata.
     * <p>
     * This method should be called whenever we consume a TABLE_MAP event, and every transaction in the log should include one
     * of these for each table affected by the transaction. Each table map event includes a monotonically-increasing numeric
     * identifier, and this identifier is used within subsequent events within the same transaction. This table identifier can
     * change when:
     * <ol>
     * <li>the table structure is modified (e.g., via an {@code ALTER TABLE ...} command); or</li>
     * <li>MySQL rotates to a new binary log file, even if the table structure does not change.</li>
     * </ol>
     * 
     * @param event the update event; never null
     * @param source the source information; never null
     * @param recorder the consumer to which all {@link SourceRecord}s should be passed; never null
     */
    public void updateTableMetadata(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        TableMapEventData metadata = event.getData();
        long tableNumber = metadata.getTableId();
        logger.debug("Received update table metadata event: {}", event);
        if (!convertersByTableId.containsKey(tableNumber)) {
            // We haven't seen this table ID, so we need to rebuild our converter functions ...
            String serverName = source.serverName();
            String databaseName = metadata.getDatabase();
            String tableName = metadata.getTable();
            String topicName = topicSelector.getTopic(serverName, databaseName, tableName);

            // Just get the current schema, which should be up-to-date ...
            TableId tableId = new TableId(databaseName, null, tableName);
            TableSchema tableSchema = tableSchemaByTableId.get(tableId);
            logger.debug("Registering metadata for table {} with table #{}", tableId, tableNumber);
            if (tableSchema == null) {
                // We are seeing an event for a row that's in a table we don't know about, meaning the table
                // was created before the binlog was enabled (or before the point we started reading it).
                if (unknownTableIds.add(tableId)) {
                    logger.warn("Transaction affects rows in {}, for which no metadata exists. All subsequent changes to rows in this table will be ignored.",
                                tableId);
                }
            }
            // Specify the envelope structure for this table's messages ...
            Envelope envelope = Envelope.defineSchema()
                                        .withName(topicName)
                                        .withRecord(tableSchema.valueSchema())
                                        .withSource(SourceInfo.SCHEMA)
                                        .build();

            // Generate this table's insert, update, and delete converters ...
            Converter converter = new Converter() {
                @Override
                public TableId tableId() {
                    return tableId;
                }

                @Override
                public String topic() {
                    return topicName;
                }

                @Override
                public Integer partition() {
                    return null;
                }

                @Override
                public Envelope envelope() {
                    return envelope;
                }

                @Override
                public Schema keySchema() {
                    return tableSchema.keySchema();
                }

                @Override
                public Schema valueSchema() {
                    return tableSchema.valueSchema();
                }

                @Override
                public Object createKey(Serializable[] row, BitSet includedColumns) {
                    // assume all columns in the table are included ...
                    return tableSchema.keyFromColumnData(row);
                }

                @Override
                public Struct createValue(Serializable[] row, BitSet includedColumns) {
                    // assume all columns in the table are included ...
                    return tableSchema.valueFromColumnData(row);
                }
            };
            convertersByTableId.put(tableNumber, converter);
            Long previousTableNumber = tableNumbersByTableName.put(tableName, tableNumber);
            if (previousTableNumber != null) {
                convertersByTableId.remove(previousTableNumber);
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("Skipping update table metadata event: {}", event);
        }
    }

    public void handleInsert(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        WriteRowsEventData write = event.getData();
        long tableNumber = write.getTableId();
        BitSet includedColumns = write.getIncludedColumns();
        Converter converter = convertersByTableId.get(tableNumber);
        if (converter != null) {
            TableId tableId = converter.tableId();
            if (tableFilter.test(tableId)) {
                logger.debug("Processing insert row event for {}: {}", tableId, event);
                String topic = converter.topic();
                Integer partitionNum = converter.partition();
                List<Serializable[]> rows = write.getRows();
                Long ts = clock.currentTimeInMillis();
                for (int row = 0; row != rows.size(); ++row) {
                    Serializable[] values = rows.get(row);
                    Schema keySchema = converter.keySchema();
                    Object key = converter.createKey(values, includedColumns);
                    Struct value = converter.createValue(values, includedColumns);
                    if (value != null || key != null) {
                        Envelope envelope = converter.envelope();
                        Map<String, ?> partition = source.partition();
                        Map<String, ?> offset = source.offset(row);
                        Struct origin = source.struct();
                        SourceRecord record = new SourceRecord(partition, offset, topic, partitionNum,
                                keySchema, key, envelope.schema(), envelope.create(value, origin, ts));
                        recorder.accept(record);
                    }
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Skipping insert row event: {}", event);
            }
        } else {
            logger.warn("Unable to find converter for table #{} in {}", tableNumber, convertersByTableId);
        }
    }

    /**
     * Process the supplied event and generate any source records, adding them to the supplied consumer.
     * 
     * @param event the database change data event to be processed; never null
     * @param source the source information to use in the record(s); never null
     * @param recorder the consumer of all source records; never null
     */
    public void handleUpdate(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        UpdateRowsEventData update = event.getData();
        long tableNumber = update.getTableId();
        BitSet includedColumns = update.getIncludedColumns();
        BitSet includedColumnsBefore = update.getIncludedColumnsBeforeUpdate();
        Converter converter = convertersByTableId.get(tableNumber);
        if (converter != null) {
            TableId tableId = converter.tableId();
            if (tableFilter.test(tableId)) {
                logger.debug("Processing update row event for {}: {}", tableId, event);
                String topic = converter.topic();
                Integer partitionNum = converter.partition();
                Long ts = clock.currentTimeInMillis();
                List<Entry<Serializable[], Serializable[]>> rows = update.getRows();
                for (int row = 0; row != rows.size(); ++row) {
                    Map.Entry<Serializable[], Serializable[]> changes = rows.get(row);
                    Serializable[] before = changes.getKey();
                    Serializable[] after = changes.getValue();
                    Schema keySchema = converter.keySchema();
                    Object key = converter.createKey(after, includedColumns);
                    Object oldKey = converter.createKey(before, includedColumns);
                    Struct valueBefore = converter.createValue(before, includedColumnsBefore);
                    Struct valueAfter = converter.createValue(after, includedColumns);
                    if (valueAfter != null || key != null) {
                        Envelope envelope = converter.envelope();
                        Map<String, ?> partition = source.partition();
                        Map<String, ?> offset = source.offset(row);
                        Struct origin = source.struct();
                        if (key != null && !Objects.equals(key, oldKey)) {
                            // The key has indeed changed, so first send a create event ...
                            SourceRecord record = new SourceRecord(partition, offset, topic, partitionNum,
                                    keySchema, key, envelope.schema(), envelope.create(valueAfter, origin, ts));
                            recorder.accept(record);

                            // then send a delete event for the old key ...
                            record = new SourceRecord(partition, offset, topic, partitionNum,
                                    keySchema, oldKey, envelope.schema(), envelope.delete(valueBefore, origin, ts));
                            recorder.accept(record);

                            // Send a tombstone event for the old key ...
                            record = new SourceRecord(partition, offset, topic, partitionNum, keySchema, oldKey, null, null);
                            recorder.accept(record);
                        } else {
                            // The key has not changed, so a simple update is fine ...
                            SourceRecord record = new SourceRecord(partition, offset, topic, partitionNum,
                                    keySchema, key, envelope.schema(), envelope.update(valueBefore, valueAfter, origin, ts));
                            recorder.accept(record);
                        }
                    }
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Skipping update row event: {}", event);
            }
        } else {
            logger.warn("Unable to find converter for table #{} in {}", tableNumber, convertersByTableId);
        }
    }

    public void handleDelete(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        DeleteRowsEventData deleted = event.getData();
        long tableNumber = deleted.getTableId();
        BitSet includedColumns = deleted.getIncludedColumns();
        Converter converter = convertersByTableId.get(tableNumber);
        if (converter != null) {
            TableId tableId = converter.tableId();
            if (tableFilter.test(tableId)) {
                logger.debug("Processing delete row event for {}: {}", tableId, event);
                String topic = converter.topic();
                Integer partitionNum = converter.partition();
                Long ts = clock.currentTimeInMillis();
                List<Serializable[]> rows = deleted.getRows();
                for (int row = 0; row != rows.size(); ++row) {
                    Serializable[] values = rows.get(row);
                    Schema keySchema = converter.keySchema();
                    Object key = converter.createKey(values, includedColumns);
                    Struct value = converter.createValue(values, includedColumns);
                    if (value != null || key != null) {
                        Envelope envelope = converter.envelope();
                        Map<String, ?> partition = source.partition();
                        Map<String, ?> offset = source.offset(row);
                        Struct origin = source.struct();
                        SourceRecord record = new SourceRecord(partition, offset, topic, partitionNum,
                                keySchema, key, envelope.schema(), envelope.delete(value, origin, ts));
                        recorder.accept(record);
                        // And send a tombstone ...
                        record = new SourceRecord(partition, offset, topic, partitionNum,
                                keySchema, key, null, null);
                        recorder.accept(record);
                    }
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Skipping delete row event: {}", event);
            }
        } else {
            logger.warn("Unable to find converter for table #{} in {}", tableNumber, convertersByTableId);
        }
    }

    protected static interface Converter {
        TableId tableId();

        String topic();

        Integer partition();

        Schema keySchema();

        Schema valueSchema();

        Envelope envelope();

        Object createKey(Serializable[] row, BitSet includedColumns);

        Struct createValue(Serializable[] row, BitSet includedColumns);
    }
}

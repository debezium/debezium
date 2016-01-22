/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.mysql.MySqlDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;

/**
 * @author Randall Hauch
 *
 */
@NotThreadSafe
final class TableConverters {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TopicSelector topicSelector;
    private final MySqlDdlParser ddlParser;
    private final Tables tables;
    private final TableSchemaBuilder schemaBuilder = new TableSchemaBuilder();
    private final Consumer<Set<TableId>> tablesChangedHandler;
    private final Map<String, TableSchema> tableSchemaByTableName = new HashMap<>();
    private final Map<Long, Converter> convertersByTableId = new HashMap<>();
    private final Map<String, Long> tableNumbersByTableName = new HashMap<>();

    public TableConverters( TopicSelector topicSelector, Tables tables, Consumer<Set<TableId>> tablesChangedHandler ) {
        this.topicSelector = topicSelector;
        this.tablesChangedHandler = tablesChangedHandler != null ? tablesChangedHandler : (ids)->{};
        this.tables = tables != null ? tables : new Tables();
        this.ddlParser = new MySqlDdlParser(false); // don't include views
    }
    
    public void updateTableCommand(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        QueryEventData command = event.getData();
        String ddlStatements = command.getSql();
        try {
            this.ddlParser.parse(ddlStatements, tables);
        } catch ( ParsingException e) {
            logger.error("Error parsing DDL statement and updating tables", e);
        } finally {
            // Figure out what changed ...
            Set<TableId> changes = tables.drainChanges();
            changes.forEach(tableId->{
                Table table = tables.forTable(tableId);
                if ( table == null ) {  // removed
                    tableSchemaByTableName.remove(tableId.table());
                } else {
                    TableSchema schema = schemaBuilder.create(table, false);
                    tableSchemaByTableName.put(tableId.table(), schema);
                }
            });
            tablesChangedHandler.accept(changes); // notify
        }
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
        if (!convertersByTableId.containsKey(tableNumber)) {
            // We haven't seen this table ID, so we need to rebuild our converter functions ...
            String databaseName = metadata.getDatabase();
            String tableName = metadata.getTable();
            String topicName = topicSelector.getTopic(databaseName, tableName);

            // Just get the current schema, which should be up-to-date ...
            TableSchema tableSchema = tableSchemaByTableName.get(tableName);

            // Generate this table's insert, update, and delete converters ...
            Converter converter = new Converter() {
                @Override
                public String topic() {
                    return topicName;
                }
                @Override
                public Integer partition() {
                    return null;
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
                public Struct inserted(Serializable[] row, BitSet includedColumns) {
                    // assume all columns in the table are included ...
                    return tableSchema.valueFromColumnData(row);
                }
                @Override
                public Struct updated(Serializable[] after, BitSet includedColumns, Serializable[] before,
                                      BitSet includedColumnsBeforeUpdate) {
                    // assume all columns in the table are included, and we'll write out only the updates ...
                    return tableSchema.valueFromColumnData(after);
                }
                @Override
                public Struct deleted(Serializable[] deleted, BitSet includedColumns) {
                    // TODO: Should we write out the old values or null?
                    // assume all columns in the table are included ...
                    return null; // tableSchema.valueFromColumnData(row);
                }
            };
            convertersByTableId.put(tableNumber, converter);
            Long previousTableNumber = tableNumbersByTableName.put(tableName, tableNumber);
            if ( previousTableNumber != null ) {
                convertersByTableId.remove(previousTableNumber);
            }
        }
    }

    public void handleInsert(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        WriteRowsEventData write = event.getData();
        long tableNumber = write.getTableId();
        BitSet includedColumns = write.getIncludedColumns();
        Converter converter = convertersByTableId.get(tableNumber);
        String topic = converter.topic();
        Integer partition = converter.partition();
        for (int row = 0; row <= source.eventRowNumber(); ++row) {
            Serializable[] values = write.getRows().get(row);
            Schema keySchema = converter.keySchema();
            Object key = converter.createKey(values,includedColumns);
            Schema valueSchema = converter.valueSchema();
            Struct value = converter.inserted(values,includedColumns);
            SourceRecord record = new SourceRecord(source.partition(), source.offset(row), topic, partition,
                    keySchema, key, valueSchema, value);
            recorder.accept(record);
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
        String topic = converter.topic();
        Integer partition = converter.partition();
        for (int row = 0; row <= source.eventRowNumber(); ++row) {
            Map.Entry<Serializable[], Serializable[]> changes = update.getRows().get(row);
            Serializable[] before = changes.getKey();
            Serializable[] after = changes.getValue();
            Schema keySchema = converter.keySchema();
            Object key = converter.createKey(after,includedColumns);
            Schema valueSchema = converter.valueSchema();
            Struct value = converter.updated(before,includedColumnsBefore, after,includedColumns);
            SourceRecord record = new SourceRecord(source.partition(), source.offset(row), topic, partition,
                    keySchema, key, valueSchema, value);
            recorder.accept(record);
        }
    }

    public void handleDelete(Event event, SourceInfo source, Consumer<SourceRecord> recorder) {
        DeleteRowsEventData deleted = event.getData();
        long tableNumber = deleted.getTableId();
        BitSet includedColumns = deleted.getIncludedColumns();
        Converter converter = convertersByTableId.get(tableNumber);
        String topic = converter.topic();
        Integer partition = converter.partition();
        for (int row = 0; row <= source.eventRowNumber(); ++row) {
            Serializable[] values = deleted.getRows().get(row);
            Schema keySchema = converter.keySchema();
            Object key = converter.createKey(values,includedColumns);
            Schema valueSchema = converter.valueSchema();
            Struct value = converter.inserted(values,includedColumns);
            SourceRecord record = new SourceRecord(source.partition(), source.offset(row), topic, partition,
                    keySchema, key, valueSchema, value);
            recorder.accept(record);
        }
    }

    protected static interface Converter {
        String topic();
        
        Integer partition();

        Schema keySchema();

        Schema valueSchema();

        Object createKey(Serializable[] row, BitSet includedColumns);

        Struct inserted(Serializable[] row, BitSet includedColumns);

        Struct updated(Serializable[] after, BitSet includedColumns, Serializable[] before, BitSet includedColumnsBeforeUpdate );

        Struct deleted(Serializable[] deleted, BitSet includedColumns);
    }
}

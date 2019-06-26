/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.avro.Schema;

import java.util.function.Consumer;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
public class RecordMaker {
    private final boolean emitTombstoneOnDelete;
    private final Filters filters;

    public RecordMaker(boolean emitTombstoneOnDelete, Filters filters) {
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
        this.filters = filters;
    }

    public void insert(SourceInfo source, RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(source, data, keySchema, valueSchema, markOffset, consumer, Record.Operation.INSERT);
    }

    public void update(SourceInfo source, RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(source, data, keySchema, valueSchema, markOffset, consumer, Record.Operation.UPDATE);
    }

    public void delete(SourceInfo source, RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(source, data, keySchema, valueSchema, markOffset, consumer, Record.Operation.DELETE);
    }

    private void createRecord(SourceInfo source, RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer, Record.Operation operation) {
        FieldFilterSelector.FieldFilter fieldFilter = filters.getFieldFilter(source.keyspaceTable);
        RowData filteredData;
        switch (operation) {
            case INSERT:
            case UPDATE:
                filteredData = fieldFilter.apply(data);
                break;
            case DELETE:
            default:
                filteredData = data;
                break;
        }

        ChangeRecord record = new ChangeRecord(source, filteredData, keySchema, valueSchema, operation, markOffset);
        consumer.accept(record);

        if (operation == Record.Operation.DELETE && emitTombstoneOnDelete) {
            // generate kafka tombstone event
            TombstoneRecord tombstoneRecord = new TombstoneRecord(source, filteredData, keySchema);
            consumer.accept(tombstoneRecord);
        }
    }
}

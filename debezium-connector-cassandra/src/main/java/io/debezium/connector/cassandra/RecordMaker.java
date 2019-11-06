/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.kafka.connect.data.Schema;

import java.util.function.Consumer;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
public class RecordMaker {
    private final boolean emitTombstoneOnDelete;
    private final Filters filters;
    private final SourceInfo sourceInfo;

    public RecordMaker(boolean emitTombstoneOnDelete, Filters filters, SourceInfo sourceInfo) {
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
        this.filters = filters;
        this.sourceInfo = sourceInfo;
    }

    public void insert(RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(data, keySchema, valueSchema, markOffset, consumer, Record.Operation.INSERT);
    }

    public void update(RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(data, keySchema, valueSchema, markOffset, consumer, Record.Operation.UPDATE);
    }

    public void delete(RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer) {
        createRecord(data, keySchema, valueSchema, markOffset, consumer, Record.Operation.DELETE);
    }

    private void createRecord(RowData data, Schema keySchema, Schema valueSchema, boolean markOffset, Consumer<Record> consumer, Record.Operation operation) {
        FieldFilterSelector.FieldFilter fieldFilter = filters.getFieldFilter(sourceInfo.keyspaceTable);
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

        ChangeRecord record = new ChangeRecord(sourceInfo, filteredData, keySchema, valueSchema, operation, markOffset);
        consumer.accept(record);

        if (operation == Record.Operation.DELETE && emitTombstoneOnDelete) {
            // generate kafka tombstone event
            TombstoneRecord tombstoneRecord = new TombstoneRecord(sourceInfo, filteredData, keySchema);
            consumer.accept(tombstoneRecord);
        }
    }

    public SourceInfo getSourceInfo() {
        return this.sourceInfo;
    }

}

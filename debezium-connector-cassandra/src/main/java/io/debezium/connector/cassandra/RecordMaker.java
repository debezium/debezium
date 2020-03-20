/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.function.BlockingConsumer;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
public class RecordMaker {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordMaker.class);
    private final boolean emitTombstoneOnDelete;
    private final Filters filters;
    private final CassandraConnectorConfig config;

    public RecordMaker(boolean emitTombstoneOnDelete, Filters filters, CassandraConnectorConfig config) {
        this.emitTombstoneOnDelete = emitTombstoneOnDelete;
        this.filters = filters;
        this.config = config;
    }

    public void insert(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.INSERT);
    }

    public void update(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.UPDATE);
    }

    public void delete(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                       Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                       boolean markOffset, BlockingConsumer<Record> consumer) {
        createRecord(cluster, offsetPosition, keyspaceTable, snapshot, tsMicro,
                data, keySchema, valueSchema, markOffset, consumer, Record.Operation.DELETE);
    }

    private void createRecord(String cluster, OffsetPosition offsetPosition, KeyspaceTable keyspaceTable, boolean snapshot,
                              Instant tsMicro, RowData data, Schema keySchema, Schema valueSchema,
                              boolean markOffset, BlockingConsumer<Record> consumer, Record.Operation operation) {
        FieldFilterSelector.FieldFilter fieldFilter = filters.getFieldFilter(keyspaceTable);
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

        SourceInfo source = new SourceInfo(config, cluster, offsetPosition, keyspaceTable, snapshot, tsMicro);
        ChangeRecord record = new ChangeRecord(source, filteredData, keySchema, valueSchema, operation, markOffset);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            LOGGER.error("Interruption while enqueuing Change Event {}", record.toString());
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }

        if (operation == Record.Operation.DELETE && emitTombstoneOnDelete) {
            // generate kafka tombstone event
            TombstoneRecord tombstoneRecord = new TombstoneRecord(source, filteredData, keySchema);
            try {
                consumer.accept(tombstoneRecord);
            }
            catch (Exception e) {
                LOGGER.error("Interruption while enqueuing Tombstone Event {}", record.toString());
                throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
            }
        }
    }

}

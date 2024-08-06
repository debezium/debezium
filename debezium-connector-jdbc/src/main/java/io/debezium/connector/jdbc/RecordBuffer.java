/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;

/**
 * A buffer of {@link SinkRecordDescriptor}. It contains the logic of when is the time to flush
 *
 * @author Mario Fiore Vitale
 */
public class RecordBuffer implements Buffer {

    private final JdbcSinkConnectorConfig connectorConfig;
    private Schema keySchema;
    private Schema valueSchema;
    private final ArrayList<SinkRecordDescriptor> records = new ArrayList<>();

    public RecordBuffer(JdbcSinkConnectorConfig connectorConfig) {

        this.connectorConfig = connectorConfig;
    }

    public List<SinkRecordDescriptor> add(SinkRecordDescriptor recordDescriptor) {

        List<SinkRecordDescriptor> flushed = new ArrayList<>();
        boolean isSchemaChanged = false;

        if (records.isEmpty()) {
            keySchema = recordDescriptor.getKeySchema();
            valueSchema = recordDescriptor.getValueSchema();
        }

        if (!Objects.equals(keySchema, recordDescriptor.getKeySchema()) || !Objects.equals(valueSchema, recordDescriptor.getValueSchema())) {
            keySchema = recordDescriptor.getKeySchema();
            valueSchema = recordDescriptor.getValueSchema();
            flushed = flush();
            isSchemaChanged = true;
        }

        records.add(recordDescriptor);

        if (isSchemaChanged) {
            // current record is already added in internal buffer after flush
            // just return the flushed buffer ignoring buffer size check
            return flushed;
        }

        if (records.size() >= connectorConfig.getBatchSize()) {
            flushed = flush();
        }

        return flushed;
    }

    public List<SinkRecordDescriptor> flush() {

        List<SinkRecordDescriptor> flushed = new ArrayList<>(records);
        records.clear();

        return flushed;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}

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
public class RecordBuffer {

    private final JdbcSinkConnectorConfig connectorConfig;
    private Schema keySchema;
    private Schema valueSchema;
    private final ArrayList<SinkRecordDescriptor> records = new ArrayList<>();

    public RecordBuffer(JdbcSinkConnectorConfig connectorConfig) {

        this.connectorConfig = connectorConfig;
    }

    public List<SinkRecordDescriptor> add(SinkRecordDescriptor recordDescriptor) {

        ArrayList<SinkRecordDescriptor> flushed = new ArrayList<>();

        if (records.isEmpty()) {
            keySchema = recordDescriptor.getKeySchema();
            valueSchema = recordDescriptor.getValueSchema();
        }

        if (!Objects.equals(keySchema, recordDescriptor.getKeySchema()) || !Objects.equals(valueSchema, recordDescriptor.getValueSchema())) {
            keySchema = recordDescriptor.getKeySchema();
            valueSchema = recordDescriptor.getValueSchema();
            flushed.addAll(flush());
        }

        records.add(recordDescriptor);

        if (records.size() >= connectorConfig.getBatchSize()) {
            flushed.addAll(flush());
        }

        return flushed;
    }

    public List<SinkRecordDescriptor> flush() {

        ArrayList<SinkRecordDescriptor> flushed = new ArrayList<>(records);
        records.clear();

        return flushed;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}

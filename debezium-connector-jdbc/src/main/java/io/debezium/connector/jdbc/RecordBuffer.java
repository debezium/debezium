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
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordBuffer {

    private final JdbcSinkConnectorConfig connectorConfig;
    private Schema keySchema;
    private Schema valueSchema;
    private final ArrayList<SinkRecord> records = new ArrayList<>();

    public RecordBuffer(JdbcSinkConnectorConfig connectorConfig) {

        this.connectorConfig = connectorConfig;
    }

    public List<SinkRecord> add(SinkRecord record) {

        ArrayList<SinkRecord> flushed = new ArrayList<>();

        if (records.isEmpty()) {
            keySchema = record.keySchema();
            valueSchema = record.valueSchema();
        }

        if (!Objects.equals(keySchema, record.keySchema()) || !Objects.equals(valueSchema, record.valueSchema())) {
            flushed.addAll(flush());
        }

        records.add(record);

        if (records.size() >= connectorConfig.getBatchSize()) {
            flushed.addAll(flush());
        }

        return flushed;
    }

    public List<SinkRecord> flush() {

        ArrayList<SinkRecord> flushed = new ArrayList<>(records);
        records.clear();

        return flushed;
    }
}

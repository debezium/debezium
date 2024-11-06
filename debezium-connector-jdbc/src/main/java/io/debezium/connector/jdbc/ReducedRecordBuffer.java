/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * A reduced implementation buffer of {@link JdbcSinkRecord}.
 * It reduces events in buffer before submit to external database.
 *
 * @author Gaurav Miglani
 */
public class ReducedRecordBuffer implements Buffer {

    private final JdbcSinkConnectorConfig connectorConfig;
    private Schema keySchema;
    private Schema valueSchema;

    private final Map<Struct, JdbcSinkRecord> records = new HashMap<>();

    public ReducedRecordBuffer(JdbcSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public List<JdbcSinkRecord> add(JdbcSinkRecord record) {
        List<JdbcSinkRecord> flushed = new ArrayList<>();
        boolean isSchemaChanged = false;

        if (records.isEmpty()) {
            keySchema = record.keySchema();
            valueSchema = record.valueSchema();
        }

        if (!Objects.equals(keySchema, record.keySchema()) || !Objects.equals(valueSchema, record.valueSchema())) {
            keySchema = record.keySchema();
            valueSchema = record.valueSchema();
            flushed = flush();
            isSchemaChanged = true;
        }

        Struct keyStruct = record.getKeyStruct(connectorConfig.getPrimaryKeyMode());
        if (keyStruct != null) {
            records.put(keyStruct, record);
        }
        else {
            throw new ConnectException("No struct-based primary key defined for record key/value, reduction buffer require struct based primary key");
        }

        if (isSchemaChanged) {
            // current record is already added in internal buffer after flush,
            // just return the flushed buffer ignoring buffer size check
            return flushed;
        }

        if (records.size() >= connectorConfig.getBatchSize()) {
            flushed = flush();
        }

        return flushed;
    }

    @Override
    public List<JdbcSinkRecord> flush() {
        List<JdbcSinkRecord> flushed = new ArrayList<>(records.values());
        records.clear();
        return flushed;
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }
}

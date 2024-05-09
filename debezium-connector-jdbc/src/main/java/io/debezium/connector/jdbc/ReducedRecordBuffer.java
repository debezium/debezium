/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.HashMap;

/**
 * A reduced implementation buffer of {@link SinkRecordDescriptor}.
 * It reduces events in buffer before submit to external database.
 *
 * @author Gaurav Miglani
 */
public class ReducedRecordBuffer implements Buffer {

    private final JdbcSinkConnectorConfig connectorConfig;
    private Schema keySchema;
    private Schema valueSchema;

    private final Map<Struct, SinkRecordDescriptor> records = new HashMap<>();

    public ReducedRecordBuffer(JdbcSinkConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
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

        Struct keyStruct = recordDescriptor.getKeyStruct(connectorConfig.getPrimaryKeyMode());
        if (keyStruct != null) {
            records.put(keyStruct, recordDescriptor);
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
    public List<SinkRecordDescriptor> flush() {
        List<SinkRecordDescriptor> flushed = new ArrayList<>(records.values());
        records.clear();
        return flushed;
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }
}

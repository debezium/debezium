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

        Struct keyStruct = recordDescriptor.getKeyStruct(connectorConfig.getPrimaryKeyMode());
        if (keyStruct != null) {
            records.put(keyStruct, recordDescriptor);
        } else {
            throw new ConnectException("No struct-based primary key defined for record key/value, reduction buffer require struct based primary key");
        }

        if (records.size() >= connectorConfig.getBatchSize()) {
            flushed.addAll(flush());
        }

        return flushed;
    }

    @Override
    public List<SinkRecordDescriptor> flush() {
        ArrayList<SinkRecordDescriptor> flushed = new ArrayList<>(records.values());
        records.clear();
        return flushed;
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }
}

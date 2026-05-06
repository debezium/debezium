/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * Base class for keyed buffer implementations that validates record keys
 * before delegating to the concrete enqueue strategy.
 *
 * @author rk3rn3r
 */
public abstract class AbstractKeyedBuffer extends AbstractBuffer {

    public AbstractKeyedBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    @Override
    public void enqueue(CollectionId collectionId, DebeziumSinkRecord record) {
        if (record.isTruncate()) {
            records.put(new BatchKey(collectionId, record.key()), new BatchRecord(collectionId, record));
            return;
        }

        if (!(record instanceof KafkaDebeziumSinkRecord kafkaDebeziumRecord)) {
            throw new RuntimeException("Keyed buffer requires KafkaDebeziumRecord type record. \"" + record.getClass() + "\" given.");
        }

        Struct struct = kafkaDebeziumRecord.getFilteredKey(connectorConfig.getPrimaryKeyMode(), connectorConfig.getPrimaryKeyFields(),
                connectorConfig.fieldFilter());
        if (null == struct || !Schema.Type.STRUCT.equals(struct.schema().type())) {
            throw new RuntimeException("No struct-based primary key defined for record key/value, keyed buffer requires struct based primary key");
        }

        doEnqueue(collectionId, record);
    }

    protected abstract void doEnqueue(CollectionId collectionId, DebeziumSinkRecord record);
}

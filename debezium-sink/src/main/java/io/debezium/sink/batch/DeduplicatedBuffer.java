/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import org.apache.kafka.connect.data.Struct;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;

/**
 * A buffer that fills a batch that deduplicates {@link DebeziumSinkRecord}s based on primary key.
 * It requires the key set to a unique value for the table.
 *
 * @author rk3rn3r
 */
public class DeduplicatedBuffer extends AbstractBuffer implements Buffer {

    public DeduplicatedBuffer(SinkConnectorConfig connectorConfig) {
        super(connectorConfig);

    }

    @Override
    public void enqueue(DebeziumSinkRecord record) {
        if (!(record instanceof KafkaDebeziumSinkRecord kafkaDebeziumRecord)) {
            throw new RuntimeException("DeduplicatedBuffer requires KafkaDebeziumRecord type record. \"" + record.getClass() + "\" given.");
        }

        /*
         * @FIXME This will only work for single-partitioned topics when done here. Needs to be done in the writer instead.
         * private final Map<CollectionId, Schema> keySchemas = new ConcurrentHashMap<>();
         * private final Map<CollectionId, Schema> valueSchemas = new ConcurrentHashMap<>();
         * boolean isSchemaChanged = false;
         * var keySchema = keySchemas.computeIfAbsent(collectionId, k -> record.keySchema());
         * var valueSchema = valueSchemas.computeIfAbsent(collectionId, k -> record.valueSchema());
         *
         * if (!Objects.equals(keySchema, record.keySchema()) || !Objects.equals(valueSchema, record.valueSchema())) {
         * keySchemas.put(collectionId, record.keySchema());
         * valueSchemas.put(collectionId, record.valueSchema());
         * isSchemaChanged = true;
         * }
         */

        Struct keyStruct = kafkaDebeziumRecord.getKeyStruct(connectorConfig.getPrimaryKeyMode());
        if (null == keyStruct) {
            throw new RuntimeException("No struct-based primary key defined for record key/value, reduction buffer require struct based primary key");
        }

        // Check if the record exists by key and update it if found
        for (int i = 0; i < records.size(); i++) {
            if (keyStruct.equals(records.get(i).getKeyStruct(connectorConfig.getPrimaryKeyMode()))) {
                records.put(i, record);
                return;
            }
        }
        records.put(record.key(), record);
    }
}

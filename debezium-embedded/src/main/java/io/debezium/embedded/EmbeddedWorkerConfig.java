/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;

import io.debezium.config.Field;

/**
 * Extension to Kafka's {@link WorkerConfig} with additions needed by {@link io.debezium.engine.DebeziumEngine}
 * and required {@link WorkerConfig} fields which are not used by {@link io.debezium.engine.DebeziumEngine}.
 * Should be removed once {@link io.debezium.engine.DebeziumEngine} is independent on Kafka model (DBZ-6234).
 */
class EmbeddedWorkerConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    static {
        ConfigDef config = baseConfigDef();
        Field.group(config, "file", EmbeddedEngineConfig.OFFSET_STORAGE_FILE_FILENAME);
        Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_TOPIC);
        Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_PARTITIONS);
        Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR);
        CONFIG = config;
    }

    protected EmbeddedWorkerConfig(Map<String, String> props) {
        super(CONFIG, addRequiredWorkerConfig(props));
    }

    /**
     * Add extra fields that are required for validation of a worker config, but that are not used within
     * the embedded engine (since the source records are never serialized) ...
     */
    protected static Map<String, String> addRequiredWorkerConfig(Map<String, String> props) {
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        return props;
    }
}

package io.debezium.embedded;

import java.util.Map;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;

/**
 * Extension to Kafka's {@link WorkerConfig} with additions needed by {@link io.debezium.engine.DebeziumEngine}.
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
        super(CONFIG, props);
    }
}

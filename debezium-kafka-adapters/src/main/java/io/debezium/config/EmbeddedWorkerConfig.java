/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension to Kafka's {@link WorkerConfig} with additions needed by {@link io.debezium.engine.DebeziumEngine}
 * and required {@link WorkerConfig} fields which are not used by {@link io.debezium.engine.DebeziumEngine}.
 * Should be removed once {@link io.debezium.engine.DebeziumEngine} is independent on Kafka model (DBZ-6234).
 */
public class EmbeddedWorkerConfig extends WorkerConfig {
    private final Logger LOGGER = LoggerFactory.getLogger(EmbeddedWorkerConfig.class);

    private static final ConfigDef CONFIG;

    static {
        ConfigDef config = baseConfigDef();
        // we cannot use EmbeddedEngineConfig fields here as it would result into circular artifact dependency.
        Field.group(config, "file", Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG));
        Field.group(config, "kafka", Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG));
        Field.group(config, "kafka", Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG));
        Field.group(config, "kafka", Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG));
        CONFIG = config;
    }

    public EmbeddedWorkerConfig(Map<String, String> props) {
        super(CONFIG, addRequiredWorkerConfig(props));
    }

    /**
     * Creates Debezium {@link Configuration} from the original values of this worker config.
     */
    public Configuration asDebeziumConfig() {
        return Configuration.from(this.originalsStrings());
    }

    /**
     * Add extra fields that are required for validation of a worker config, but that are not used within
     * the embedded engine (since the source records are never serialized) ...
     */
    protected static Map<String, String> addRequiredWorkerConfig(Map<String, String> props) {
        props.putIfAbsent(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        return props;
    }
}

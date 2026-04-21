/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.kafka.KafkaOffsetStoreAdapter;

/**
 * Provider for Kafka Connect's {@link KafkaOffsetBackingStore}.
 * <p>
 * This provider wraps the Kafka Connect implementation, handling the initialization
 * of the required {@link SharedTopicAdmin} and {@link JsonConverter} for offset storage in Kafka topics.
 * <p>
 * This requires a Kafka cluster and appropriate configuration for bootstrap servers,
 * offset storage topic, partitions, and replication factor.
 *
 * @author Debezium Authors
 */
public class KafkaOffsetStoreProvider implements OffsetStoreProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetStoreProvider.class);

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public OffsetStore create(Configuration config, Map<String, String> connectorConfig) {
        LOGGER.debug("Creating Kafka KafkaOffsetBackingStore");

        final String clientId = "debezium-server";
        final Map<String, Object> adminProps = new HashMap<>(connectorConfig);
        adminProps.put(CLIENT_ID_CONFIG, clientId + "shared-admin");

        // Validate required configuration
        Stream.of(
                DistributedConfig.BOOTSTRAP_SERVERS_CONFIG,
                DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG,
                DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG,
                DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
                .forEach(prop -> {
                    if (!adminProps.containsKey(prop)) {
                        throw new DebeziumException(String.format(
                                "Cannot initialize Kafka offset storage, mandatory configuration option '%s' is missing",
                                prop));
                    }
                });

        // Create shared topic admin for managing offset topics
        SharedTopicAdmin sharedAdmin = new SharedTopicAdmin(adminProps);

        // Create JSON converter for offset serialization
        final Converter converter = createConverter();

        // Create Kafka Connect KafkaOffsetBackingStore and wrap it
        org.apache.kafka.connect.storage.KafkaOffsetBackingStore kafkaStore = new KafkaOffsetBackingStore(sharedAdmin, () -> clientId, converter);
        return new KafkaOffsetStoreAdapter(kafkaStore);
    }

    @Override
    public String getOffsetStoreClassName() {
        return KafkaOffsetBackingStore.class.getName();
    }

    /**
     * Creates a JSON converter configured for offset storage.
     * Schema support is disabled as offsets are simple key-value maps.
     */
    private Converter createConverter() {
        final JsonConverter converter = new JsonConverter();
        converter.configure(
                Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"),
                true // isKey = true for offset storage
        );
        return converter;
    }
}

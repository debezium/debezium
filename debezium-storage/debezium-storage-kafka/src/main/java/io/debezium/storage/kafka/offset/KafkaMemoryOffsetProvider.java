/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.kafka.KafkaOffsetStoreAdapter;

/**
 * Provider for Kafka Connect's {@link MemoryOffsetBackingStore}.
 * <p>
 * This provider wraps the Kafka Connect implementation for in-memory offset storage,
 * which is useful for testing and ephemeral deployments.
 *
 * @author Debezium Authors
 */
public class KafkaMemoryOffsetProvider implements OffsetStoreProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMemoryOffsetProvider.class);

    @Override
    public String getName() {
        return "memory";
    }

    @Override
    public OffsetStore create(Configuration config, Map<String, String> connectorConfig) {
        LOGGER.debug("Creating Kafka MemoryOffsetBackingStore");

        // Create MemoryOffsetBackingStore with connectorPartitions override
        // This method is required by Kafka Connect but not used in embedded mode
        MemoryOffsetBackingStore kafkaStore = new MemoryOffsetBackingStore() {
            @Override
            public Set<Map<String, Object>> connectorPartitions(String connectorName) {
                // Not used in embedded engine context
                return null;
            }
        };
        return new KafkaOffsetStoreAdapter(kafkaStore);
    }

    @Override
    public String getOffsetStoreClassName() {
        return MemoryOffsetBackingStore.class.getName();
    }
}

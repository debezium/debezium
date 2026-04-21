/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.kafka.KafkaOffsetStoreAdapter;

/**
 * Provider for Kafka Connect's {@link FileOffsetBackingStore}.
 * <p>
 * This provider wraps the Kafka Connect implementation, handling the initialization
 * of the required {@link JsonConverter} for offset serialization.
 *
 * @author Debezium Authors
 */
public class KafkaFileOffsetProvider implements OffsetStoreProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFileOffsetProvider.class);

    @Override
    public String getName() {
        return "file";
    }

    @Override
    public OffsetStore create(Configuration config, Map<String, String> connectorConfig) {
        LOGGER.debug("Creating Kafka FileOffsetBackingStore");

        // Create JSON converter for offset serialization
        final Converter converter = createConverter();

        // Create Kafka Connect FileOffsetBackingStore and wrap it
        FileOffsetBackingStore kafkaStore = new FileOffsetBackingStore(converter);
        return new KafkaOffsetStoreAdapter(kafkaStore);
    }

    @Override
    public String getOffsetStoreClassName() {
        return FileOffsetBackingStore.class.getName();
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

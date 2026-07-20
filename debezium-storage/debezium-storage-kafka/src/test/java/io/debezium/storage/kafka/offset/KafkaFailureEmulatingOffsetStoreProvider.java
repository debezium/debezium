/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.kafka.KafkaConnectOffsetStoreAdapter;

/**
 * Provider that creates a file-based offset store wrapped with {@link FailureEmulatingOffsetBackingStore}
 * to emulate offset storage failures. Useful for testing engine resilience to storage errors.
 */
public class KafkaFailureEmulatingOffsetStoreProvider implements OffsetStoreProvider {

    public static final String NAME = "failureEmulatingFileStore";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public OffsetStore create(Configuration config) {
        final JsonConverter converter = new JsonConverter();
        converter.configure(
                Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        FileOffsetBackingStore kafkaStore = new FileOffsetBackingStore(converter);
        FailureEmulatingOffsetBackingStore failureStore = new FailureEmulatingOffsetBackingStore(kafkaStore);

        return new KafkaConnectOffsetStoreAdapter(failureStore,
                KafkaOffsetStoreConverter.jsonConverter(true),
                KafkaOffsetStoreConverter.jsonConverter(false));
    }

    @Override
    public Optional<String> getOffsetStoreClassName() {
        return Optional.of(FailureEmulatingOffsetBackingStore.class.getName());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.offset;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;
import io.debezium.storage.kafka.KafkaConnectOffsetStoreAdapter;

/**
 * Provider that creates an offset store backed by {@link InterruptingOffsetBackingStore},
 * which throws {@link InterruptedException} on offset commits.
 * Useful for testing engine shutdown behavior on interrupts.
 */
public class KafkaInterruptingOffsetStoreProvider implements OffsetStoreProvider {

    public static final String NAME = "interruptingStore";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public OffsetStore create(Configuration config) {
        return new KafkaConnectOffsetStoreAdapter(new InterruptingOffsetBackingStore(),
                KafkaOffsetStoreConverter.jsonConverter(true),
                KafkaOffsetStoreConverter.jsonConverter(false));
    }

    @Override
    public Optional<String> getOffsetStoreClassName() {
        return Optional.of(InterruptingOffsetBackingStore.class.getName());
    }
}

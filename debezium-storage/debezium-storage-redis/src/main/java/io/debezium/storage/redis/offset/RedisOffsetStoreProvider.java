/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.spi.storage.OffsetStore;
import io.debezium.spi.storage.OffsetStoreProvider;

public class RedisOffsetStoreProvider implements OffsetStoreProvider {

    @Override
    public String getName() {
        return "redis";
    }

    @Override
    public OffsetStore create(Configuration config) {
        return new RedisOffsetBackingStore();
    }

    @Override
    public Optional<String> getOffsetStoreClassName() {
        return Optional.of(RedisOffsetBackingStore.class.getName());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deprecated and replaced with {@link io.debezium.storage.redis.offset.RedisOffsetBackingStore}
 *
 */

@Deprecated
public class RedisOffsetBackingStore extends io.debezium.storage.redis.offset.RedisOffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisOffsetBackingStore.class);

    public RedisOffsetBackingStore() {
        LOGGER.warn("Class '{}' is deprecated and scheduled for removal, please use '{}'",
                RedisOffsetBackingStore.class.getName(),
                io.debezium.storage.redis.offset.RedisOffsetBackingStore.class.getName());
    }
}

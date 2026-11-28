/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

import io.debezium.config.EnumeratedValue;

/**
 * The Redis driver library used to communicate with Redis.
 */
public enum RedisClientLibrary implements EnumeratedValue {

    /**
     * Use the Jedis driver. Supports both single instance and cluster mode.
     */
    JEDIS("jedis"),

    /**
     * Use the Lettuce driver. Supports single instance mode only and requires
     * {@code io.lettuce:lettuce-core} on the classpath.
     */
    LETTUCE("lettuce");

    private final String value;

    RedisClientLibrary(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * Determine the library matching the given value.
     *
     * @param value the configuration property value; may be null
     * @return the matching library, or null if no match was found
     */
    public static RedisClientLibrary parse(String value) {
        if (value == null) {
            return null;
        }
        for (RedisClientLibrary option : values()) {
            if (option.getValue().equalsIgnoreCase(value.trim())) {
                return option;
            }
        }
        return null;
    }
}

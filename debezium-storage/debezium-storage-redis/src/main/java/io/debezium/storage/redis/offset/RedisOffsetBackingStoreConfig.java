/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.offset;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.storage.redis.RedisCommonConfig;
import io.debezium.util.Collect;

public class RedisOffsetBackingStoreConfig extends RedisCommonConfig {

    private static final String PROP_PREFIX = "offset.storage.";

    private static final String DEFAULT_REDIS_KEY_NAME = "metadata:debezium:offsets";
    private static final Field PROP_KEY_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "key")
            .withDescription("The Redis key that will be used to store the offsets")
            .withDefault(DEFAULT_REDIS_KEY_NAME);

    private String redisKeyName;

    public RedisOffsetBackingStoreConfig(Configuration config) {
        super(config, PROP_PREFIX);
    }

    @Override
    protected void init(Configuration config) {
        super.init(config);
        this.redisKeyName = config.getString(PROP_KEY_NAME);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_KEY_NAME);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public String getRedisKeyName() {
        return redisKeyName;
    }

}
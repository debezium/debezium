/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.history;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.redis.RedisCommonConfig;
import io.debezium.util.Collect;

public class RedisSchemaHistoryConfig extends RedisCommonConfig {

    private static final String DEFAULT_REDIS_KEY_NAME = "metadata:debezium:schema_history";
    private static final Field PROP_KEY_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "key")
            .withDescription("The Redis key that will be used to store the database schema history")
            .withDefault(DEFAULT_REDIS_KEY_NAME);

    private String redisKeyName;

    public RedisSchemaHistoryConfig(Configuration config) {
        super(config, SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING);
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
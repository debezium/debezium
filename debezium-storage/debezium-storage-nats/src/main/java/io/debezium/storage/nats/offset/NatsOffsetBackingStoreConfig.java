/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.offset;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;

/**
 * Configuration for NATS-based offset backing store.
 *
 * Keys follow the same pattern as Redis: all NATS-specific keys start with
 * "nats." and are chained with the module prefix "offset.storage.".
 * For example: offset.storage.nats.url, offset.storage.nats.bucket.name, ...
 *
 * @author Nick Babcock
 */
public class NatsOffsetBackingStoreConfig extends NatsCommonConfig {

    private static final String PROP_PREFIX = "offset.storage.";

    public static final Field PROP_BUCKET_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "bucket.name")
            .withDescription("The name of the NATS Object Store bucket to store offsets")
            .withDefault("debezium-offsets");

    public static final Field PROP_RETRY_ENABLED = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.enabled")
            .withDescription("Whether to retry failed offset operations")
            .withDefault(true);

    public static final Field PROP_RETRY_DELAY_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "retry.delay.ms")
            .withDescription("Delay between retry attempts for failed offset operations in milliseconds")
            .withDefault(1000L);

    public static final Field PROP_MAX_RETRIES = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.retries")
            .withDescription("Maximum number of retry attempts for failed offset operations")
            .withDefault(3);

    private String bucketName;
    private boolean retryEnabled;
    private long retryDelayMs;
    private int maxRetries;

    public NatsOffsetBackingStoreConfig(Configuration config) {
        super(config, PROP_PREFIX);
    }

    @Override
    protected void init(Configuration c) {
        super.init(c);
        this.bucketName = c.getString(PROP_BUCKET_NAME);
        this.retryEnabled = c.getBoolean(PROP_RETRY_ENABLED);
        this.retryDelayMs = c.getLong(PROP_RETRY_DELAY_MS);
        this.maxRetries = c.getInteger(PROP_MAX_RETRIES);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(
                PROP_BUCKET_NAME,
                PROP_RETRY_ENABLED,
                PROP_RETRY_DELAY_MS,
                PROP_MAX_RETRIES);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public String getBucketName() {
        return bucketName;
    }

    public boolean isRetryEnabled() {
        return retryEnabled;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    // Non-configurable scope used to distinguish this component in shared NATS
    // connection cache
    public static final String NATS_INSTANCE_SCOPE_PREFIX = "offset";

    public String instanceScope() {
        return NATS_INSTANCE_SCOPE_PREFIX + ":" + getBucketName();
    }

}

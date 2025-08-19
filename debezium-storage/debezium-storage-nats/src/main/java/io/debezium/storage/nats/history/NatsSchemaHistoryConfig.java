/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;

/**
 * Configuration for NATS-based schema history storage.
 *
 * Keys follow the same pattern as Redis: all NATS-specific keys start with
 * "nats." and are chained with the module prefix
 * {@link SchemaHistory#CONFIGURATION_FIELD_PREFIX_STRING}.
 * For example: schema.history.internal.nats.url,
 * schema.history.internal.nats.stream.name, ...
 *
 * @author Nick Babcock
 */
public class NatsSchemaHistoryConfig extends NatsCommonConfig {

    public static final Field PROP_STREAM_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "stream.name")
            .withDescription("The name of the NATS JetStream stream to store schema history")
            .withDefault("debezium-schema-history");

    public static final Field PROP_SUBJECT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "subject")
            .withDescription("The NATS subject to publish schema history records")
            .withDefault("debezium.schema.history");

    public static final Field PROP_STORAGE_TYPE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "storage.type")
            .withDescription("The storage type for the JetStream stream (file or memory)")
            .withDefault("file");

    public static final Field PROP_REPLICAS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "replicas")
            .withDescription("Number of replicas for the JetStream stream")
            .withDefault(1);

    public static final Field PROP_MAX_AGE_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.age.ms")
            .withDescription("Maximum age of messages in the stream in milliseconds (0 for unlimited)")
            .withDefault(0L); // 0 means unlimited

    public static final Field PROP_MAX_BYTES = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.bytes")
            .withDescription("Maximum bytes for the stream (-1 for unlimited)")
            .withDefault(-1L); // -1 means unlimited

    public static final Field PROP_RECOVERY_POLL_INTERVAL_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "recovery.poll.interval.ms")
            .withDescription("Interval for polling during schema history recovery in milliseconds")
            .withDefault(100L);

    public static final Field PROP_RECOVERY_ATTEMPTS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "recovery.attempts")
            .withDescription("Maximum number of attempts for schema history recovery")
            .withDefault(100);

    private String streamName;
    private String subject;
    private String storageType;
    private int replicas;
    private long maxAgeMs;
    private long maxBytes;
    private long recoveryPollIntervalMs;
    private int recoveryAttempts;

    public NatsSchemaHistoryConfig(Configuration config) {
        super(config, SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING);
    }

    @Override
    protected void init(Configuration c) {
        super.init(c);
        this.streamName = c.getString(PROP_STREAM_NAME);
        this.subject = c.getString(PROP_SUBJECT);
        this.storageType = c.getString(PROP_STORAGE_TYPE);
        this.replicas = c.getInteger(PROP_REPLICAS);
        this.maxAgeMs = c.getLong(PROP_MAX_AGE_MS);
        this.maxBytes = c.getLong(PROP_MAX_BYTES);
        this.recoveryPollIntervalMs = c.getLong(PROP_RECOVERY_POLL_INTERVAL_MS);
        this.recoveryAttempts = c.getInteger(PROP_RECOVERY_ATTEMPTS);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(
                PROP_STREAM_NAME,
                PROP_SUBJECT,
                PROP_STORAGE_TYPE,
                PROP_REPLICAS,
                PROP_MAX_AGE_MS,
                PROP_MAX_BYTES,
                PROP_RECOVERY_POLL_INTERVAL_MS,
                PROP_RECOVERY_ATTEMPTS);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getSubject() {
        return subject;
    }

    public String getStorageType() {
        return storageType;
    }

    public int getReplicas() {
        return replicas;
    }

    public long getMaxAgeMs() {
        return maxAgeMs;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getRecoveryPollIntervalMs() {
        return recoveryPollIntervalMs;
    }

    public int getRecoveryAttempts() {
        return recoveryAttempts;
    }

    // Non-configurable scope used to distinguish this component in shared NATS
    // connection cache
    public static final String NATS_INSTANCE_SCOPE_PREFIX = "schema";

    public String instanceScope() {
        return NATS_INSTANCE_SCOPE_PREFIX + ":" + getStreamName();
    }
}

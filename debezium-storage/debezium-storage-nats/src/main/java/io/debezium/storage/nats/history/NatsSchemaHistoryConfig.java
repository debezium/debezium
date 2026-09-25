/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import java.util.List;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.nats.NatsCommonConfig;
import io.debezium.util.Collect;
import io.nats.client.support.NatsJetStreamConstants;

/**
 * Configuration for NATS-based schema history storage.
 *
 * Keys follow the same pattern as Redis: all NATS-specific keys start with
 * "nats." and are chained with the module prefix
 * {@link SchemaHistory#CONFIGURATION_FIELD_PREFIX_STRING}.
 * For example: schema.history.internal.nats.url,
 * schema.history.internal.nats.stream.name, ...
 *
 * <p>Note: the stream retention defaults (unlimited age and size) match the
 * behavior of the file-based schema history. Setting {@link #PROP_MAX_AGE_MS} or
 * {@link #PROP_MAX_BYTES} bounds the growth of the stream, but the stream then
 * discards its oldest schema history records, and a later recovery can only
 * rebuild a partial schema. Bound it only if the connector can tolerate that.
 * {@link #PROP_DUPLICATE_WINDOW_MS} is what keeps a retried publish from being
 * recorded twice.
 *
 * @author Nick Chomey
 */
public class NatsSchemaHistoryConfig extends NatsCommonConfig {

    /**
     * The JetStream storage type for the schema history stream.
     */
    public enum StorageType implements EnumeratedValue {
        FILE("file"),
        MEMORY("memory");

        private final String value;

        StorageType(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public static final Field PROP_STREAM_NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "stream.name")
            .withDescription("The name of the NATS JetStream stream to store schema history")
            .withDefault("debezium-schema-history");

    public static final Field PROP_SUBJECT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "subject")
            .withDescription("The NATS subject to publish schema history records")
            .withDefault("debezium.schema.history");

    public static final Field PROP_STORAGE_TYPE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "storage.type")
            .withDescription("The storage type for the JetStream stream (file or memory)")
            .withEnum(StorageType.class, StorageType.FILE);

    public static final Field PROP_REPLICAS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "replicas")
            .withDescription("Number of replicas for the JetStream stream")
            .withDefault(1);

    public static final Field PROP_MAX_AGE_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.age.ms")
            .withDescription("Maximum age of messages in the stream in milliseconds (0 for unlimited). "
                    + "A limit discards the oldest schema history records, so a later recovery can only rebuild "
                    + "a partial schema")
            .withDefault(0L); // 0 means unlimited

    public static final Field PROP_MAX_BYTES = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "max.bytes")
            .withDescription("Maximum bytes for the stream (-1 for unlimited). "
                    + "A limit discards the oldest schema history records, so a later recovery can only rebuild "
                    + "a partial schema")
            .withDefault(-1L); // -1 means unlimited

    public static final Field PROP_DUPLICATE_WINDOW_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "duplicate.window.ms")
            .withDescription("The JetStream duplicate window in milliseconds: the period during which the stream "
                    + "discards a message whose ID it has already seen. This stops a schema history record from being "
                    + "stored twice when a publish is retried after a lost acknowledgement. A zero or negative value "
                    + "leaves the server default in place, which is what the server substitutes for a window of zero")
            .withDefault(NatsJetStreamConstants.SERVER_DEFAULT_DUPLICATE_WINDOW_MS);

    public static final Field PROP_RECOVERY_POLL_INTERVAL_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "recovery.poll.interval.ms")
            .withDescription("Interval for polling during schema history recovery in milliseconds")
            .withDefault(100L);

    public static final Field PROP_RECOVERY_TIMEOUT_MS = Field
            .create(CONFIGURATION_FIELD_PREFIX_STRING + "recovery.timeout.ms")
            .withDescription("Maximum time to spend recovering the schema history in milliseconds")
            .withDefault(60000L);

    private String streamName;
    private String subject;
    private StorageType storageType;
    private int replicas;
    private long maxAgeMs;
    private long maxBytes;
    private long duplicateWindowMs;
    private long recoveryPollIntervalMs;
    private long recoveryTimeoutMs;

    public NatsSchemaHistoryConfig(Configuration config) {
        super(config, SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING);
    }

    @Override
    protected void init(Configuration c) {
        super.init(c);
        this.streamName = c.getString(PROP_STREAM_NAME);
        this.subject = c.getString(PROP_SUBJECT);
        this.storageType = EnumeratedValue.parse(StorageType.class, c.getString(PROP_STORAGE_TYPE),
                PROP_STORAGE_TYPE.defaultValueAsString());
        this.replicas = c.getInteger(PROP_REPLICAS);
        this.maxAgeMs = c.getLong(PROP_MAX_AGE_MS);
        this.maxBytes = c.getLong(PROP_MAX_BYTES);
        this.duplicateWindowMs = c.getLong(PROP_DUPLICATE_WINDOW_MS);
        this.recoveryPollIntervalMs = c.getLong(PROP_RECOVERY_POLL_INTERVAL_MS);
        this.recoveryTimeoutMs = c.getLong(PROP_RECOVERY_TIMEOUT_MS);
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
                PROP_DUPLICATE_WINDOW_MS,
                PROP_RECOVERY_POLL_INTERVAL_MS,
                PROP_RECOVERY_TIMEOUT_MS);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getSubject() {
        return subject;
    }

    public StorageType getStorageType() {
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

    public long getDuplicateWindowMs() {
        return duplicateWindowMs;
    }

    public long getRecoveryPollIntervalMs() {
        return recoveryPollIntervalMs;
    }

    public long getRecoveryTimeoutMs() {
        return recoveryTimeoutMs;
    }
}

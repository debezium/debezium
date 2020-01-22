/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * Configuration options common to all Debezium connectors.
 *
 * @author Gunnar Morling
 */
public abstract class CommonConnectorConfig {

    /**
     * The set of predefined versions e.g. for source struct maker version
     */
    public enum Version implements EnumeratedValue {
        V1("v1"),
        V2("v2");

        private final String value;

        private Version(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static Version parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (Version option : Version.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static Version parse(String value, String defaultValue) {
            Version mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final int DEFAULT_MAX_QUEUE_SIZE = 8192;
    public static final int DEFAULT_MAX_BATCH_SIZE = 2048;
    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 500;
    public static final String DATABASE_CONFIG_PREFIX = "database.";

    public static final Field TOMBSTONES_ON_DELETE = Field.create("tombstones.on.delete")
            .withDisplayName("Change the behaviour of Debezium with regards to delete operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Whether delete operations should be represented by a delete event and a subsquent" +
                    "tombstone event (true) or only by a delete event (false). Emitting the tombstone event (the" +
                    " default behavior) allows Kafka to completely delete all events pertaining to the given key once" +
                    " the source record got deleted.");

    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size")
            .withDisplayName("Change event buffer size")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to "
                    + DEFAULT_MAX_QUEUE_SIZE + ", and should always be larger than the maximum batch size.")
            .withDefault(DEFAULT_MAX_QUEUE_SIZE)
            .withValidation(CommonConnectorConfig::validateMaxQueueSize);

    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size")
            .withDisplayName("Change event batch size")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of each batch of source records. Defaults to " + DEFAULT_MAX_BATCH_SIZE + ".")
            .withDefault(DEFAULT_MAX_BATCH_SIZE)
            .withValidation(Field::isPositiveInteger);

    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
            .withDisplayName("Poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Frequency in milliseconds to wait for new change events to appear after receiving no events. Defaults to " + DEFAULT_POLL_INTERVAL_MILLIS + "ms.")
            .withDefault(DEFAULT_POLL_INTERVAL_MILLIS)
            .withValidation(Field::isPositiveInteger);

    public static final Field SNAPSHOT_DELAY_MS = Field.create("snapshot.delay.ms")
            .withDisplayName("Snapshot Delay (milliseconds)")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("The number of milliseconds to delay before a snapshot will begin.")
            .withDefault(0L)
            .withValidation(Field::isNonNegativeLong);

    public static final Field SNAPSHOT_FETCH_SIZE = Field.create("snapshot.fetch.size")
            .withDisplayName("Snapshot fetch size")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum number of records that should be loaded into memory while performing a snapshot")
            .withValidation(Field::isNonNegativeInteger);

    public static final Field SOURCE_STRUCT_MAKER_VERSION = Field.create("source.struct.version")
            .withDisplayName("Source struct maker version")
            .withEnum(Version.class, Version.V2)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("A version of the format of the publicly visible source part in the message")
            .withValidation(Field::isClassName);

    public static final Field SANITIZE_FIELD_NAMES = Field.create("sanitize.field.names")
            .withDisplayName("Sanitize field names to adhere to Avro naming conventions")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Whether field names will be sanitized to Avro naming conventions")
            .withDefault(Boolean.FALSE);

    public static final Field PROVIDE_TRANSACTION_METADATA = Field.create("provide.transaction.metadata")
            .withDisplayName("Store transaction metadata information in a dedicated topic.")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Enables transaction metadata extraction together with event counting")
            .withDefault(Boolean.FALSE);

    private final Configuration config;
    private final boolean emitTombstoneOnDelete;
    private final int maxQueueSize;
    private final int maxBatchSize;
    private final Duration pollInterval;
    private final String logicalName;
    private final String heartbeatTopicsPrefix;
    private final Duration snapshotDelayMs;
    private final int snapshotFetchSize;
    private final SourceInfoStructMaker<? extends AbstractSourceInfo> sourceInfoStructMaker;
    private final boolean sanitizeFieldNames;
    private final boolean shouldProvideTransactionMetadata;

    protected CommonConnectorConfig(Configuration config, String logicalName, int defaultSnapshotFetchSize) {
        this.config = config;
        this.emitTombstoneOnDelete = config.getBoolean(CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        this.maxQueueSize = config.getInteger(MAX_QUEUE_SIZE);
        this.maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        this.pollInterval = config.getDuration(POLL_INTERVAL_MS, ChronoUnit.MILLIS);
        this.logicalName = logicalName;
        this.heartbeatTopicsPrefix = config.getString(Heartbeat.HEARTBEAT_TOPICS_PREFIX);
        this.snapshotDelayMs = Duration.ofMillis(config.getLong(SNAPSHOT_DELAY_MS));
        this.snapshotFetchSize = config.getInteger(SNAPSHOT_FETCH_SIZE, defaultSnapshotFetchSize);
        this.sourceInfoStructMaker = getSourceInfoStructMaker(Version.parse(config.getString(SOURCE_STRUCT_MAKER_VERSION)));
        this.sanitizeFieldNames = config.getBoolean(SANITIZE_FIELD_NAMES) || isUsingAvroConverter(config);
        this.shouldProvideTransactionMetadata = config.getBoolean(PROVIDE_TRANSACTION_METADATA);
    }

    /**
     * Provides access to the "raw" config instance. In most cases, access via typed getters for individual properties
     * on the connector config class should be preferred.
     */
    public Configuration getConfig() {
        return config;
    }

    public boolean isEmitTombstoneOnDelete() {
        return emitTombstoneOnDelete;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public String getLogicalName() {
        return logicalName;
    }

    public abstract String getContextName();

    public String getHeartbeatTopicsPrefix() {
        return heartbeatTopicsPrefix;
    }

    public Duration getSnapshotDelay() {
        return snapshotDelayMs;
    }

    public int getSnapshotFetchSize() {
        return snapshotFetchSize;
    }

    public boolean shouldProvideTransactionMetadata() {
        return shouldProvideTransactionMetadata;
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractSourceInfo> SourceInfoStructMaker<T> getSourceInfoStructMaker() {
        return (SourceInfoStructMaker<T>) sourceInfoStructMaker;
    }

    public boolean getSanitizeFieldNames() {
        return sanitizeFieldNames;
    }

    private static int validateMaxQueueSize(Configuration config, Field field, Field.ValidationOutput problems) {
        int maxQueueSize = config.getInteger(field);
        int maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        int count = 0;
        if (maxQueueSize <= 0) {
            problems.accept(field, maxQueueSize, "A positive queue size is required");
            ++count;
        }
        if (maxQueueSize <= maxBatchSize) {
            problems.accept(field, maxQueueSize, "Must be larger than the maximum batch size");
            ++count;
        }
        return count;
    }

    private static boolean isUsingAvroConverter(Configuration config) {
        final String avroConverter = "io.confluent.connect.avro.AvroConverter";
        final String keyConverter = config.getString("key.converter");
        final String valueConverter = config.getString("value.converter");
        return avroConverter.equals(keyConverter) || avroConverter.equals(valueConverter);
    }

    protected static int validateServerNameIsDifferentFromHistoryTopicName(Configuration config, Field field, ValidationOutput problems) {
        String serverName = config.getString(field);
        String historyTopicName = config.getString(KafkaDatabaseHistory.TOPIC);

        if (Objects.equals(serverName, historyTopicName)) {
            problems.accept(field, serverName, "Must not have the same value as " + KafkaDatabaseHistory.TOPIC.name());
            return 1;
        }

        return 0;
    }

    /**
     * Returns the connector-specific {@link SourceInfoStructMaker} based on the given configuration.
     */
    protected abstract SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version);
}

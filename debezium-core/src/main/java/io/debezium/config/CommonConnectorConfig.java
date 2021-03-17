/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.history.KafkaDatabaseHistory;
import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.util.Strings;

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

        Version(String value) {
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

    /**
     * The set of predefined modes for dealing with failures during event processing.
     */
    public enum EventProcessingFailureHandlingMode implements EnumeratedValue {

        /**
         * Problematic events will be skipped.
         */
        SKIP("skip"),

        /**
         * The position of problematic events will be logged and events will be skipped.
         */
        WARN("warn"),

        /**
         * An exception indicating the problematic events and their position is raised, causing the connector to be stopped.
         */
        FAIL("fail"),

        /**
         * Problematic events will be skipped - for transitional period only, scheduled to be removed.
         */
        IGNORE("ignore");

        public static final String OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING = "ignore";

        private final String value;

        EventProcessingFailureHandlingMode(String value) {
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
        public static EventProcessingFailureHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }

            value = value.trim();

            // backward compatibility, will be removed in 1.2
            if (OBSOLETE_NAME_FOR_SKIP_FAILURE_HANDLING.equals(value)) {
                return SKIP;
            }

            for (EventProcessingFailureHandlingMode option : EventProcessingFailureHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }
    }

    /**
     * The set of predefined BinaryHandlingMode options or aliases
     */
    public enum BinaryHandlingMode implements EnumeratedValue {

        /**
         * Represent binary values as byte array
         */
        BYTES("bytes", SchemaBuilder::bytes),

        /**
         * Represent binary values as base64-encoded string
         */
        BASE64("base64", SchemaBuilder::string),

        /**
         * Represents binary values as hex-encoded (base16) string
         */
        HEX("hex", SchemaBuilder::string);

        private final String value;
        private final Supplier<SchemaBuilder> schema;

        BinaryHandlingMode(String value, Supplier<SchemaBuilder> schema) {
            this.value = value;
            this.schema = schema;
        }

        @Override
        public String getValue() {
            return value;
        }

        public SchemaBuilder getSchema() {
            return schema.get();
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static BinaryHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (BinaryHandlingMode option : BinaryHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static BinaryHandlingMode parse(String value, String defaultValue) {
            BinaryHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    private static final String CONFLUENT_AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
    private static final String APICURIO_AVRO_CONVERTER = "io.apicurio.registry.utils.converter.AvroConverter";

    public static final int DEFAULT_MAX_QUEUE_SIZE = 8192;
    public static final int DEFAULT_MAX_BATCH_SIZE = 2048;
    public static final int DEFAULT_QUERY_FETCH_SIZE = 0;
    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 500;
    public static final String DATABASE_CONFIG_PREFIX = "database.";
    private static final String CONVERTER_TYPE_SUFFIX = ".type";
    public static final long DEFAULT_RETRIABLE_RESTART_WAIT = 10000L;
    public static final long DEFAULT_MAX_QUEUE_SIZE_IN_BYTES = 0; // In case we don't want to pass max.queue.size.in.bytes;

    public static final Field RETRIABLE_RESTART_WAIT = Field.create("retriable.restart.connector.wait.ms")
            .withDisplayName("Retriable restart wait (ms)")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_RETRIABLE_RESTART_WAIT)
            .withDescription(
                    "Time to wait before restarting connector after retriable exception occurs. Defaults to " + DEFAULT_RETRIABLE_RESTART_WAIT + "ms.")
            .withValidation(Field::isPositiveLong);

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
            .withDescription("Time to wait for new change events to appear after receiving no events, given in milliseconds. Defaults to 500 ms.")
            .withDefault(DEFAULT_POLL_INTERVAL_MILLIS)
            .withValidation(Field::isPositiveInteger);

    public static final Field MAX_QUEUE_SIZE_IN_BYTES = Field.create("max.queue.size.in.bytes")
            .withDisplayName("Change event buffer size in bytes")
            .withType(Type.LONG)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of the queue in bytes for change events read from the database log but not yet recorded or forwarded. Defaults to "
                    + DEFAULT_MAX_QUEUE_SIZE_IN_BYTES + ". Mean the feature is not enabled")
            .withDefault(DEFAULT_MAX_QUEUE_SIZE_IN_BYTES)
            .withValidation(Field::isNonNegativeLong);

    public static final Field SNAPSHOT_DELAY_MS = Field.create("snapshot.delay.ms")
            .withDisplayName("Snapshot Delay (milliseconds)")
            .withType(Type.LONG)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("A delay period before a snapshot will begin, given in milliseconds. Defaults to 0 ms.")
            .withDefault(0L)
            .withValidation(Field::isNonNegativeLong);

    public static final Field SNAPSHOT_FETCH_SIZE = Field.create("snapshot.fetch.size")
            .withDisplayName("Snapshot fetch size")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum number of records that should be loaded into memory while performing a snapshot")
            .withValidation(Field::isNonNegativeInteger);

    public static final Field SNAPSHOT_MODE_TABLES = Field.create("snapshot.include.collection.list")
            .withDisplayName("Snapshot mode include data collection")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription(
                    "this setting must be set to specify a list of tables/collections whose snapshot must be taken on creating or restarting the connector.");

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

    public static final Field EVENT_PROCESSING_FAILURE_HANDLING_MODE = Field.create("event.processing.failure.handling.mode")
            .withDisplayName("Event deserialization failure handling")
            .withEnum(EventProcessingFailureHandlingMode.class, EventProcessingFailureHandlingMode.FAIL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify how failures during processing of events (i.e. when encountering a corrupted event) should be handled, including:"
                    + "'fail' (the default) an exception indicating the problematic event and its position is raised, causing the connector to be stopped; "
                    + "'warn' the problematic event and its position will be logged and the event will be skipped;"
                    + "'ignore' the problematic event will be skipped.");

    public static final Field CUSTOM_CONVERTERS = Field.create("converters")
            .withDisplayName("List of prefixes defining custom values converters.")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Optional list of custom converters that would be used instead of default ones. "
                    + "The converters are defined using '<converter.prefix>.type' config option and configured using options '<converter.prefix>.<option>'");

    public static final Field SKIPPED_OPERATIONS = Field.create("skipped.operations")
            .withDisplayName("skipped Operations")
            .withType(Type.LIST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withValidation(CommonConnectorConfig::validateSkippedOperation)
            .withDescription("The comma-separated list of operations to skip during streaming, defined as: 'c' for inserts/create; 'u' for updates; 'd' for deletes. "
                    + "By default, no operations will be skipped.");

    public static final Field BINARY_HANDLING_MODE = Field.create("binary.handling.mode")
            .withDisplayName("Binary Handling")
            .withEnum(BinaryHandlingMode.class, BinaryHandlingMode.BYTES)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Specify how binary (blob, binary, etc.) columns should be represented in change events, including:"
                    + "'bytes' represents binary data as byte array (default)"
                    + "'base64' represents binary data as base64-encoded string"
                    + "'hex' represents binary data as hex-encoded (base16) string");

    public static final Field QUERY_FETCH_SIZE = Field.create("query.fetch.size")
            .withDisplayName("Query fetch size")
            .withType(Type.INT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The maximum number of records that should be loaded into memory while streaming.  A value of `0` uses the default JDBC fetch size.")
            .withValidation(Field::isNonNegativeInteger)
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field SNAPSHOT_MAX_THREADS = Field.create("snapshot.max.threads")
            .withDisplayName("Snapshot maximum threads")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(1)
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum number of threads used to perform the snapshot.  Defaults to 1.");

    public static final Field SIGNAL_DATA_COLLECTION = Field.create("signal.data.collection")
            .withDisplayName("Signaling data collection")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The name of the data collection that is used to send signals/commands to Debezium. Signaling is disabled when not set.");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .connector(
                    EVENT_PROCESSING_FAILURE_HANDLING_MODE,
                    MAX_BATCH_SIZE,
                    MAX_QUEUE_SIZE,
                    POLL_INTERVAL_MS,
                    MAX_QUEUE_SIZE_IN_BYTES,
                    PROVIDE_TRANSACTION_METADATA,
                    SKIPPED_OPERATIONS,
                    SNAPSHOT_DELAY_MS,
                    SNAPSHOT_MODE_TABLES,
                    SNAPSHOT_FETCH_SIZE,
                    SNAPSHOT_MAX_THREADS,
                    RETRIABLE_RESTART_WAIT,
                    QUERY_FETCH_SIZE)
            .events(
                    CUSTOM_CONVERTERS,
                    SANITIZE_FIELD_NAMES,
                    TOMBSTONES_ON_DELETE,
                    SOURCE_STRUCT_MAKER_VERSION,
                    Heartbeat.HEARTBEAT_INTERVAL,
                    Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                    SIGNAL_DATA_COLLECTION)
            .create();

    private final Configuration config;
    private final boolean emitTombstoneOnDelete;
    private final int maxQueueSize;
    private final int maxBatchSize;
    private final long maxQueueSizeInBytes;
    private final Duration pollInterval;
    private final String logicalName;
    private final String heartbeatTopicsPrefix;
    private final Duration snapshotDelayMs;
    private final Duration retriableRestartWait;
    private final int snapshotFetchSize;
    private final int snapshotMaxThreads;
    private final Integer queryFetchSize;
    private final SourceInfoStructMaker<? extends AbstractSourceInfo> sourceInfoStructMaker;
    private final boolean sanitizeFieldNames;
    private final boolean shouldProvideTransactionMetadata;
    private final EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode;
    private final CustomConverterRegistry customConverterRegistry;
    private final BinaryHandlingMode binaryHandlingMode;
    private final String signalingDataCollection;

    protected CommonConnectorConfig(Configuration config, String logicalName, int defaultSnapshotFetchSize) {
        this.config = config;
        this.emitTombstoneOnDelete = config.getBoolean(CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        this.maxQueueSize = config.getInteger(MAX_QUEUE_SIZE);
        this.maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        this.pollInterval = config.getDuration(POLL_INTERVAL_MS, ChronoUnit.MILLIS);
        this.maxQueueSizeInBytes = config.getLong(MAX_QUEUE_SIZE_IN_BYTES);
        this.logicalName = logicalName;
        this.heartbeatTopicsPrefix = config.getString(Heartbeat.HEARTBEAT_TOPICS_PREFIX);
        this.snapshotDelayMs = Duration.ofMillis(config.getLong(SNAPSHOT_DELAY_MS));
        this.retriableRestartWait = Duration.ofMillis(config.getLong(RETRIABLE_RESTART_WAIT));
        this.snapshotFetchSize = config.getInteger(SNAPSHOT_FETCH_SIZE, defaultSnapshotFetchSize);
        this.snapshotMaxThreads = config.getInteger(SNAPSHOT_MAX_THREADS);
        this.queryFetchSize = config.getInteger(QUERY_FETCH_SIZE);
        this.sourceInfoStructMaker = getSourceInfoStructMaker(Version.parse(config.getString(SOURCE_STRUCT_MAKER_VERSION)));
        this.sanitizeFieldNames = config.getBoolean(SANITIZE_FIELD_NAMES) || isUsingAvroConverter(config);
        this.shouldProvideTransactionMetadata = config.getBoolean(PROVIDE_TRANSACTION_METADATA);
        this.eventProcessingFailureHandlingMode = EventProcessingFailureHandlingMode.parse(config.getString(EVENT_PROCESSING_FAILURE_HANDLING_MODE));
        this.customConverterRegistry = new CustomConverterRegistry(getCustomConverters());
        this.binaryHandlingMode = BinaryHandlingMode.parse(config.getString(BINARY_HANDLING_MODE));
        this.signalingDataCollection = config.getString(SIGNAL_DATA_COLLECTION);
    }

    /**
     * Provides access to the "raw" config instance. In most cases, access via typed getters for individual properties
     * on the connector config class should be preferred.
     * TODO this should be protected in the future to force proper facade methods based access / encapsulation
     */
    @Deprecated
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

    public long getMaxQueueSizeInBytes() {
        return maxQueueSizeInBytes;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public String getLogicalName() {
        return logicalName;
    }

    public abstract String getContextName();

    public abstract String getConnectorName();

    public String getHeartbeatTopicsPrefix() {
        return heartbeatTopicsPrefix;
    }

    public Duration getRetriableRestartWait() {
        return retriableRestartWait;
    }

    public Duration getSnapshotDelay() {
        return snapshotDelayMs;
    }

    public int getSnapshotFetchSize() {
        return snapshotFetchSize;
    }

    public int getSnapshotMaxThreads() {
        return snapshotMaxThreads;
    }

    public int getQueryFetchSize() {
        return queryFetchSize;
    }

    public boolean shouldProvideTransactionMetadata() {
        return shouldProvideTransactionMetadata;
    }

    public EventProcessingFailureHandlingMode getEventProcessingFailureHandlingMode() {
        return eventProcessingFailureHandlingMode;
    }

    public CustomConverterRegistry customConverterRegistry() {
        return customConverterRegistry;
    }

    @SuppressWarnings("unchecked")
    private List<CustomConverter<SchemaBuilder, ConvertedField>> getCustomConverters() {
        final String converterNameList = config.getString(CUSTOM_CONVERTERS);
        final List<String> converterNames = Strings.listOf(converterNameList, x -> x.split(","), String::trim);

        return converterNames.stream()
                .map(name -> {
                    CustomConverter<SchemaBuilder, ConvertedField> converter = config.getInstance(name + CONVERTER_TYPE_SUFFIX, CustomConverter.class);
                    converter.configure(config.subset(name, true).asProperties());
                    return converter;
                })
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractSourceInfo> SourceInfoStructMaker<T> getSourceInfoStructMaker() {
        return (SourceInfoStructMaker<T>) sourceInfoStructMaker;
    }

    public boolean getSanitizeFieldNames() {
        return sanitizeFieldNames;
    }

    public Set<Envelope.Operation> getSkippedOps() {
        String operations = config.getString(SKIPPED_OPERATIONS);

        if (operations != null) {
            return Arrays.stream(operations.split(","))
                    .map(String::trim)
                    .map(Operation::forCode)
                    .collect(Collectors.toSet());
        }
        else {
            return Collections.emptySet();
        }
    }

    public Set<String> getDataCollectionsToBeSnapshotted() {
        return Optional.ofNullable(config.getString(SNAPSHOT_MODE_TABLES))
                .map(tables -> Strings.setOf(tables, Function.identity()))
                .orElseGet(Collections::emptySet);
    }

    /**
     * @return true if the connector should emit messages about schema changes into a public facing
     * topic.
     */
    public boolean isSchemaChangesHistoryEnabled() {
        return false;
    }

    /**
     * Validates the supplied fields in this configuration. Extra fields not described by the supplied
     * {@code fields} parameter will not be validated.
     *
     * @param fields the fields
     * @param problems the consumer to eb called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    public boolean validate(Iterable<Field> fields, ValidationOutput problems) {
        return config.validate(fields, problems);
    }

    /**
     * Validate the supplied fields in this configuration. Extra fields not described by the supplied
     * {@code fields} parameter will not be validated.
     *
     * @param fields the fields
     * @param problems the consumer to be called with each problem; never null
     * @return {@code true} if the value is considered valid, or {@code false} if it is not valid
     */
    public boolean validateAndRecord(Iterable<Field> fields, Consumer<String> problems) {
        return config.validateAndRecord(fields, problems);
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

    private static int validateSkippedOperation(Configuration config, Field field, ValidationOutput problems) {
        String operations = config.getString(field);

        if (operations == null) {
            return 0;
        }

        for (String operation : operations.split(",")) {
            switch (operation.trim()) {
                case "r":
                case "c":
                case "u":
                case "d":
                    continue;
                default:
                    problems.accept(field, operation, "Invalid operation");
                    return 1;
            }
        }

        return 0;
    }

    private static boolean isUsingAvroConverter(Configuration config) {
        final String keyConverter = config.getString("key.converter");
        final String valueConverter = config.getString("value.converter");

        return CONFLUENT_AVRO_CONVERTER.equals(keyConverter) || CONFLUENT_AVRO_CONVERTER.equals(valueConverter)
                || APICURIO_AVRO_CONVERTER.equals(keyConverter) || APICURIO_AVRO_CONVERTER.equals(valueConverter);
    }

    public static int validateServerNameIsDifferentFromHistoryTopicName(Configuration config, Field field, ValidationOutput problems) {
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

    public BinaryHandlingMode binaryHandlingMode() {
        return binaryHandlingMode;
    }

    public String getSignalingDataCollectionId() {
        return signalingDataCollection;
    }
}

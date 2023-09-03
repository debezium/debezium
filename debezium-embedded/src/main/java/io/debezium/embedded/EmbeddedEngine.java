/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Instantiator;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.VariableLatch;

/**
 * A mechanism for running a single Kafka Connect {@link SourceConnector} within an application's process. An embedded connector
 * is entirely standalone and only talks with the source system; no Kafka, Kafka Connect, or Zookeeper processes are needed.
 * Applications using an embedded connector simply set one up and supply a {@link Consumer consumer function} to which the
 * connector will pass all {@link SourceRecord}s containing database change events.
 * <p>
 * With an embedded connector, the application that runs the connector assumes all responsibility for fault tolerance,
 * scalability, and durability. Additionally, applications must specify how the connector can store its relational database
 * schema history and offsets. By default, this information will be stored in memory and will thus be lost upon application
 * restart.
 * <p>
 * Embedded connectors are designed to be submitted to an {@link Executor} or {@link ExecutorService} for execution by a single
 * thread, and a running connector can be stopped either by calling {@link #stop()} from another thread or by interrupting
 * the running thread (e.g., as is the case with {@link ExecutorService#shutdownNow()}).
 *
 * @author Randall Hauch
 */
@ThreadSafe
public final class EmbeddedEngine implements DebeziumEngine<SourceRecord> {

    /**
     * A required field for an embedded connector that specifies the unique name for the connector instance.
     */
    public static final Field ENGINE_NAME = Field.create("name")
            .withDescription("Unique name for this connector instance.")
            .required();

    /**
     * A required field for an embedded connector that specifies the name of the normal Debezium connector's Java class.
     */
    public static final Field CONNECTOR_CLASS = Field.create("connector.class")
            .withDescription("The Java class for the connector")
            .required();

    /**
     * An optional field that specifies the name of the class that implements the {@link OffsetBackingStore} interface,
     * and that will be used to store offsets recorded by the connector.
     */
    public static final Field OFFSET_STORAGE = Field.create("offset.storage")
            .withDescription("The Java class that implements the `OffsetBackingStore` "
                    + "interface, used to periodically store offsets so that, upon "
                    + "restart, the connector can resume where it last left off.")
            .withDefault(FileOffsetBackingStore.class.getName());

    /**
     * An optional field that specifies the file location for the {@link FileOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_FILE_FILENAME = Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG)
            .withDescription("The file where offsets are to be stored. Required when "
                    + "'offset.storage' is set to the " +
                    FileOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the topic name for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_TOPIC = Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)
            .withDescription("The name of the Kafka topic where offsets are to be stored. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the number of partitions for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_PARTITIONS = Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG)
            .withType(ConfigDef.Type.INT)
            .withDescription("The number of partitions used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional field that specifies the replication factor for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR = Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .withType(ConfigDef.Type.SHORT)
            .withDescription("Replication factor used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withDescription("Interval at which to try committing offsets, given in milliseconds. Defaults to 1 minute (60,000 ms).")
            .withDefault(60000L)
            .withValidation(Field::isNonNegativeInteger);

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_COMMIT_TIMEOUT_MS = Field.create("offset.flush.timeout.ms")
            .withDescription("Time to wait for records to flush and partition offset data to be"
                    + " committed to offset storage before cancelling the process and restoring the offset "
                    + "data to be committed in a future attempt, given in milliseconds. Defaults to 5 seconds (5000 ms).")
            .withDefault(5000L)
            .withValidation(Field::isPositiveInteger);

    public static final Field OFFSET_COMMIT_POLICY = Field.create("offset.commit.policy")
            .withDescription("The fully-qualified class name of the commit policy type. This class must implement the interface "
                    + OffsetCommitPolicy.class.getName()
                    + ". The default is a periodic commit policy based upon time intervals.")
            .withDefault(io.debezium.embedded.spi.OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName())
            .withValidation(Field::isClassName);

    /**
     * A list of Predicates that can be assigned to transformations.
     */
    public static final Field PREDICATES = Field.create("predicates")
            .withDisplayName("List of prefixes defining predicates.")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Optional list of predicates that can be assigned to transformations. "
                    + "The predicates are defined using '<predicate.prefix>.type' config option and configured using options '<predicate.prefix>.<option>'");

    /**
     * A list of SMTs to be applied on the messages generated by the engine.
     */
    public static final Field TRANSFORMS = Field.create("transforms")
            .withDisplayName("List of prefixes defining transformations.")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Optional list of single message transformations applied on the messages. "
                    + "The transforms are defined using '<transform.prefix>.type' config option and configured using options '<transform.prefix>.<option>'");

    private static final int DEFAULT_ERROR_MAX_RETRIES = -1;

    public static final Field ERRORS_MAX_RETRIES = Field.create("errors.max.retries")
            .withDisplayName("The maximum number of retries")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(DEFAULT_ERROR_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription("The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, > 0 = num of retries).");

    public static final Field ERRORS_RETRY_DELAY_INITIAL_MS = Field.create("errors.retry.delay.initial.ms")
            .withDisplayName("Initial delay for retries")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(300)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Initial delay (in ms) for retries when encountering connection errors."
                    + " This value will be doubled upon every retry but won't exceed 'errors.retry.delay.max.ms'.");

    public static final Field ERRORS_RETRY_DELAY_MAX_MS = Field.create("errors.retry.delay.max.ms")
            .withDisplayName("Max delay between retries")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(10000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Max delay (in ms) between retries when encountering connection errors.");

    public static final Field WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS = Field.create("debezium.embedded.shutdown.pause.before.interrupt.ms")
            .withDisplayName("Time to wait to engine completion before interrupt")
            .withType(Type.LONG)
            .withDefault(Duration.ofMinutes(5).toMillis())
            .withValidation(Field::isPositiveInteger)
            .withDescription(String.format("How long we wait before forcefully stopping the connector thread when shutting down. " +
                    "Must be bigger than the time it takes two polling loops to finish ({} ms)", ChangeEventSourceCoordinator.SHUTDOWN_WAIT_TIMEOUT.toMillis() * 2));

    /**
     * The array of fields that are required by each connectors.
     */
    public static final Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

    /**
     * The array of all exposed fields.
     */
    protected static final Field.Set ALL_FIELDS = CONNECTOR_FIELDS.with(OFFSET_STORAGE, OFFSET_STORAGE_FILE_FILENAME,
            OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS,
            ERRORS_MAX_RETRIES, ERRORS_RETRY_DELAY_INITIAL_MS, ERRORS_RETRY_DELAY_MAX_MS);

    public static final class BuilderImpl implements Builder {
        private Configuration config;
        private DebeziumEngine.ChangeConsumer<SourceRecord> handler;
        private ClassLoader classLoader;
        private Clock clock;
        private DebeziumEngine.CompletionCallback completionCallback;
        private DebeziumEngine.ConnectorCallback connectorCallback;
        private OffsetCommitPolicy offsetCommitPolicy = null;

        @Override
        public Builder using(Configuration config) {
            this.config = config;
            return this;
        }

        @Override
        public Builder using(Properties config) {
            this.config = Configuration.from(config);
            return this;
        }

        @Override
        public Builder using(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        @Override
        public Builder using(Clock clock) {
            this.clock = clock;
            return this;
        }

        @Override
        public Builder using(DebeziumEngine.CompletionCallback completionCallback) {
            this.completionCallback = completionCallback;
            return this;
        }

        @Override
        public Builder using(DebeziumEngine.ConnectorCallback connectorCallback) {
            this.connectorCallback = connectorCallback;
            return this;
        }

        @Override
        public Builder using(OffsetCommitPolicy offsetCommitPolicy) {
            this.offsetCommitPolicy = offsetCommitPolicy;
            return this;
        }

        @Override
        public Builder notifying(Consumer<SourceRecord> consumer) {
            this.handler = buildDefaultChangeConsumer(consumer);
            return this;
        }

        @Override
        public Builder notifying(DebeziumEngine.ChangeConsumer<SourceRecord> handler) {
            this.handler = handler;
            if (!config.hasKey(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name()) && !handler.supportsTombstoneEvents()) {
                LOGGER.info("Consumer doesn't support tombstone events, setting '{}' to false.", CommonConnectorConfig.TOMBSTONES_ON_DELETE.name());
                config = config.edit().with(CommonConnectorConfig.TOMBSTONES_ON_DELETE, false).build();
            }
            return this;
        }

        @Override
        public Builder using(java.time.Clock clock) {
            return using(new Clock() {

                @Override
                public long currentTimeInMillis() {
                    return clock.millis();
                }
            });
        }

        @Override
        public EmbeddedEngine build() {
            if (classLoader == null) {
                classLoader = Instantiator.getClassLoader();
            }
            if (clock == null) {
                clock = Clock.system();
            }
            Objects.requireNonNull(config, "A connector configuration must be specified.");
            Objects.requireNonNull(handler, "A connector consumer or changeHandler must be specified.");
            return new EmbeddedEngine(config, classLoader, clock,
                    handler, completionCallback, connectorCallback, offsetCommitPolicy);
        }

        // backward compatibility methods
        @Override
        public Builder using(CompletionCallback completionCallback) {
            return using((DebeziumEngine.CompletionCallback) completionCallback);
        }

        @Override
        public Builder using(ConnectorCallback connectorCallback) {
            return using((DebeziumEngine.ConnectorCallback) connectorCallback);
        }
    }

    /**
     * A callback function to be notified when the connector completes.
     */
    @Deprecated
    public interface CompletionCallback extends DebeziumEngine.CompletionCallback {
    }

    /**
     * Callback function which informs users about the various stages a connector goes through during startup
     */
    @Deprecated
    public interface ConnectorCallback extends DebeziumEngine.ConnectorCallback {
    }

    /**
     * A callback function to be notified when the connector completes.
     */
    public static class CompletionResult implements CompletionCallback {
        private final CompletionCallback delegate;
        private final CountDownLatch completed = new CountDownLatch(1);
        private boolean success;
        private String message;
        private Throwable error;

        public CompletionResult() {
            this(null);
        }

        public CompletionResult(CompletionCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(boolean success, String message, Throwable error) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.completed.countDown();
            if (delegate != null) {
                delegate.handle(success, message, error);
            }
        }

        /**
         * Causes the current thread to wait until the {@link #handle(boolean, String, Throwable) completion occurs}
         * or until the thread is {@linkplain Thread#interrupt interrupted}.
         * <p>
         * This method returns immediately if the connector has completed already.
         *
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public void await() throws InterruptedException {
            this.completed.await();
        }

        /**
         * Causes the current thread to wait until the {@link #handle(boolean, String, Throwable) completion occurs},
         * unless the thread is {@linkplain Thread#interrupt interrupted}, or the specified waiting time elapses.
         * <p>
         * This method returns immediately if the connector has completed already.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the {@code timeout} argument
         * @return {@code true} if the completion was received, or {@code false} if the waiting time elapsed before the completion
         *         was received.
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.completed.await(timeout, unit);
        }

        /**
         * Determine if the connector has completed.
         *
         * @return {@code true} if the connector has completed, or {@code false} if the connector is still running and this
         *         callback has not yet been {@link #handle(boolean, String, Throwable) notified}
         */
        public boolean hasCompleted() {
            return completed.getCount() == 0;
        }

        /**
         * Get whether the connector completed normally.
         *
         * @return {@code true} if the connector completed normally, or {@code false} if the connector produced an error that
         *         prevented startup or premature termination (or the connector has not yet {@link #hasCompleted() completed})
         */
        public boolean success() {
            return success;
        }

        /**
         * Get the completion message.
         *
         * @return the completion message, or null if the connector has not yet {@link #hasCompleted() completed}
         */
        public String message() {
            return message;
        }

        /**
         * Get the completion error, if there is one.
         *
         * @return the completion error, or null if there is no error or connector has not yet {@link #hasCompleted() completed}
         */
        public Throwable error() {
            return error;
        }

        /**
         * Determine if there is a completion error.
         *
         * @return {@code true} if there is a {@link #error completion error}, or {@code false} if there is no error or
         *         the connector has not yet {@link #hasCompleted() completed}
         */
        public boolean hasError() {
            return error != null;
        }
    }

    /**
     * Contract passed to {@link ChangeConsumer}s, allowing them to commit single records as they have been processed
     * and to signal that offsets may be flushed eventually.
     */
    @ThreadSafe
    @Deprecated
    public interface RecordCommitter extends DebeziumEngine.RecordCommitter<SourceRecord> {
    }

    /**
     * A contract invoked by the embedded engine when it has received a batch of change records to be processed. Allows
     * to process multiple records in one go, acknowledging their processing once that's done.
     */
    @Deprecated
    public interface ChangeConsumer extends DebeziumEngine.ChangeConsumer<SourceRecord> {
    }

    private static ChangeConsumer buildDefaultChangeConsumer(Consumer<SourceRecord> consumer) {
        return new ChangeConsumer() {

            /**
             * the default implementation that is compatible with the old Consumer api.
             *
             * On every record, it calls the consumer, and then only marks the record
             * as processed when accept returns, additionally, it handles StopConnectorExceptions
             * and ensures that we all ways try and mark a batch as finished, even with exceptions
             * @param records the records to be processed
             * @param committer the committer that indicates to the system that we are finished
             *
             * @throws Exception
             */
            @Override
            public void handleBatch(List<SourceRecord> records, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
                for (SourceRecord record : records) {
                    try {
                        consumer.accept(record);
                        committer.markProcessed(record);
                    }
                    catch (StopConnectorException | StopEngineException ex) {
                        // ensure that we mark the record as finished
                        // in this case
                        committer.markProcessed(record);
                        throw ex;
                    }
                }
                committer.markBatchFinished();
            }
        };
    }

    /**
     * A builder to set up and create {@link EmbeddedEngine} instances.
     */
    @Deprecated
    public interface Builder extends DebeziumEngine.Builder<SourceRecord> {

        /**
         * Use the specified configuration for the connector. The configuration is assumed to already be valid.
         *
         * @param config the configuration
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(Configuration config);

        /**
         * Use the specified clock when needing to determine the current time. Passing <code>null</code> or not calling this
         * method results in the connector using the {@link Clock#system() system clock}.
         *
         * @param clock the clock
         * @return this builder object so methods can be chained together; never null
         */
        Builder using(Clock clock);

        // backward compatibility methods
        @Override
        Builder notifying(Consumer<SourceRecord> consumer);

        @Override
        Builder notifying(DebeziumEngine.ChangeConsumer<SourceRecord> handler);

        @Override
        Builder using(ClassLoader classLoader);

        Builder using(CompletionCallback completionCallback);

        Builder using(ConnectorCallback connectorCallback);

        @Override
        Builder using(OffsetCommitPolicy policy);

        @Override
        EmbeddedEngine build();
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link EmbeddedEngine} instances.
     *
     * @return the new builder; never null
     */
    @Deprecated
    public static Builder create() {
        return new BuilderImpl();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedEngine.class);

    private final Configuration config;
    private final Clock clock;
    private final ClassLoader classLoader;
    private final DebeziumEngine.ChangeConsumer<SourceRecord> handler;
    private final DebeziumEngine.CompletionCallback completionCallback;
    private final DebeziumEngine.ConnectorCallback connectorCallback;
    private final AtomicReference<Thread> runningThread = new AtomicReference<>();
    private final VariableLatch latch = new VariableLatch(0);
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final WorkerConfig workerConfig;
    private final CompletionResult completionResult;
    private long recordsSinceLastCommit = 0;
    private long timeOfLastCommitMillis = 0;
    private OffsetCommitPolicy offsetCommitPolicy;

    private SourceTask task;
    private final Transformations transformations;

    private EmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, DebeziumEngine.ChangeConsumer<SourceRecord> handler,
                           DebeziumEngine.CompletionCallback completionCallback, DebeziumEngine.ConnectorCallback connectorCallback,
                           OffsetCommitPolicy offsetCommitPolicy) {
        this.config = config;
        this.handler = handler;
        this.classLoader = classLoader;
        this.clock = clock;
        this.completionCallback = completionCallback != null ? completionCallback : (success, msg, error) -> {
            if (!success) {
                LOGGER.error(msg, error);
            }
        };
        this.connectorCallback = connectorCallback;
        this.completionResult = new CompletionResult();
        this.offsetCommitPolicy = offsetCommitPolicy;

        assert this.config != null;
        assert this.handler != null;
        assert this.classLoader != null;
        assert this.clock != null;
        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
        keyConverter.configure(internalConverterConfig, true);
        valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
        valueConverter.configure(internalConverterConfig, false);

        transformations = new Transformations(config);

        // Create the worker config, adding extra fields that are required for validation of a worker config
        // but that are not used within the embedded engine (since the source records are never serialized) ...
        Map<String, String> embeddedConfig = config.asMap(ALL_FIELDS);
        embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        workerConfig = new EmbeddedConfig(embeddedConfig);
    }

    /**
     * Determine if this embedded connector is currently running.
     *
     * @return {@code true} if running, or {@code false} otherwise
     */
    public boolean isRunning() {
        return this.runningThread.get() != null;
    }

    private void fail(String msg) {
        fail(msg, null);
    }

    private void fail(String msg, Throwable error) {
        if (completionResult.hasError()) {
            // there's already a recorded failure, so keep the original one and simply log this one
            LOGGER.error(msg, error);
            return;
        }
        // don't use the completion callback here because we want to store the error and message only
        completionResult.handle(false, msg, error);
    }

    private void succeed(String msg) {
        // don't use the completion callback here because we want to store the error and message only
        completionResult.handle(true, msg, null);
    }

    /**
     * Run this embedded connector and deliver database changes to the registered {@link Consumer}. This method blocks until
     * the connector is stopped.
     * <p>
     * First, the method checks to see if this instance is currently {@link #run() running}, and if so immediately returns.
     * <p>
     * If the configuration is valid, this method starts the connector and starts polling the connector for change events.
     * All messages are delivered in batches to the {@link Consumer} registered with this embedded connector. The batch size,
     * polling
     * frequency, and other parameters are controlled via configuration settings. This continues until this connector is
     * {@link #stop() stopped}.
     * <p>
     * Note that there are two ways to stop a connector running on a thread: calling {@link #stop()} from another thread, or
     * interrupting the thread (e.g., via {@link ExecutorService#shutdownNow()}).
     * <p>
     * This method can be called repeatedly as needed.
     */
    @Override
    public void run() {
        if (runningThread.compareAndSet(null, Thread.currentThread())) {

            final String engineName = config.getString(ENGINE_NAME);
            final String connectorClassName = config.getString(CONNECTOR_CLASS);
            final Optional<DebeziumEngine.ConnectorCallback> connectorCallback = Optional.ofNullable(this.connectorCallback);
            // Only one thread can be in this part of the method at a time ...
            latch.countUp();
            try {
                if (!config.validateAndRecord(CONNECTOR_FIELDS, LOGGER::error)) {
                    fail("Failed to start connector with invalid configuration (see logs for actual errors)");
                    return;
                }

                // Instantiate the connector ...
                SourceConnector connector = null;
                try {
                    @SuppressWarnings("unchecked")
                    Class<? extends SourceConnector> connectorClass = (Class<SourceConnector>) classLoader.loadClass(connectorClassName);
                    connector = connectorClass.getDeclaredConstructor().newInstance();
                }
                catch (Throwable t) {
                    fail("Unable to instantiate connector class '" + connectorClassName + "'", t);
                    return;
                }
                Map<String, String> connectorConfig = workerConfig.originalsStrings();
                Config validatedConnectorConfig = connector.validate(connectorConfig);
                ConfigInfos configInfos = AbstractHerder.generateResult(connectorClassName, Collections.emptyMap(), validatedConnectorConfig.configValues(),
                        connector.config().groups());
                if (configInfos.errorCount() > 0) {
                    String errors = configInfos.values().stream()
                            .flatMap(v -> v.configValue().errors().stream())
                            .collect(Collectors.joining(" "));
                    fail("Connector configuration is not valid. " + errors);
                    return;
                }

                // Instantiate the offset store ...
                final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
                OffsetBackingStore offsetStore = null;
                try {
                    @SuppressWarnings("unchecked")
                    Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
                    offsetStore = offsetStoreClass.getDeclaredConstructor().newInstance();
                }
                catch (Throwable t) {
                    fail("Unable to instantiate OffsetBackingStore class '" + offsetStoreClassName + "'", t);
                    return;
                }

                // Initialize the offset store ...
                try {
                    offsetStore.configure(workerConfig);
                    offsetStore.start();
                }
                catch (Throwable t) {
                    fail("Unable to configure and start the '" + offsetStoreClassName + "' offset backing store", t);
                    offsetStore.stop();
                    return;
                }

                // Set up the offset commit policy ...
                if (offsetCommitPolicy == null) {
                    try {
                        offsetCommitPolicy = Instantiator.getInstanceWithProperties(config.getString(EmbeddedEngine.OFFSET_COMMIT_POLICY),
                                config.asProperties());
                    }
                    catch (Throwable t) {
                        fail("Unable to instantiate OffsetCommitPolicy class '" + offsetStoreClassName + "'", t);
                        return;
                    }
                }

                OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
                        keyConverter, valueConverter);

                // Initialize the connector using a context that does NOT respond to requests to reconfigure tasks ...
                ConnectorContext context = new SourceConnectorContext() {

                    @Override
                    public void requestTaskReconfiguration() {
                        // Do nothing ...
                    }

                    @Override
                    public void raiseError(Exception e) {
                        fail(e.getMessage(), e);
                    }

                    @Override
                    public OffsetStorageReader offsetStorageReader() {
                        return offsetReader;
                    }
                };
                connector.initialize(context);
                OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                        keyConverter, valueConverter);

                Duration commitTimeout = Duration.ofMillis(config.getLong(OFFSET_COMMIT_TIMEOUT_MS));

                try {
                    // Start the connector with the given properties and get the task configurations ...
                    connector.start(connectorConfig);
                    connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStarted);
                    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
                    Class<? extends Task> taskClass = connector.taskClass();
                    if (taskConfigs.isEmpty()) {
                        String msg = "Unable to start connector's task class '" + taskClass.getName() + "' with no task configuration";
                        fail(msg);
                        return;
                    }
                    task = null;
                    try {
                        task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
                    }
                    catch (IllegalAccessException | InstantiationException t) {
                        fail("Unable to instantiate connector's task class '" + taskClass.getName() + "'", t);
                        return;
                    }
                    try {
                        SourceTaskContext taskContext = new SourceTaskContext() {
                            @Override
                            public OffsetStorageReader offsetStorageReader() {
                                return offsetReader;
                            }

                            // Purposely not marking this method with @Override as it was introduced in Kafka 2.x
                            // and otherwise would break builds based on Kafka 1.x
                            public Map<String, String> configs() {
                                // TODO Auto-generated method stub
                                return null;
                            }
                        };
                        task.initialize(taskContext);
                        task.start(taskConfigs.get(0));
                        connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStarted);
                    }
                    catch (Throwable t) {
                        // Clean-up allocated resources
                        try {
                            LOGGER.debug("Stopping the task");
                            task.stop();
                        }
                        catch (Throwable tstop) {
                            LOGGER.info("Error while trying to stop the task");
                        }
                        // Mask the passwords ...
                        Configuration config = Configuration.from(taskConfigs.get(0)).withMaskedPasswords();
                        String msg = "Unable to initialize and start connector's task class '" + taskClass.getName() + "' with config: "
                                + config;
                        fail(msg, t);
                        return;
                    }

                    recordsSinceLastCommit = 0;
                    Throwable handlerError = null, retryError = null;
                    try {
                        timeOfLastCommitMillis = clock.currentTimeInMillis();
                        RecordCommitter committer = buildRecordCommitter(offsetWriter, task, commitTimeout);
                        while (runningThread.get() != null) {
                            List<SourceRecord> changeRecords = null;
                            try {
                                LOGGER.debug("Embedded engine is polling task for records on thread {}", runningThread.get());
                                changeRecords = task.poll(); // blocks until there are values ...
                                LOGGER.debug("Embedded engine returned from polling task for records");
                            }
                            catch (InterruptedException e) {
                                // Interrupted while polling ...
                                LOGGER.debug("Embedded engine interrupted on thread {} while polling the task for records", runningThread.get());
                                if (this.runningThread.get() == Thread.currentThread()) {
                                    // this thread is still set as the running thread -> we were not interrupted
                                    // due the stop() call -> probably someone else called the interrupt on us ->
                                    // -> we should raise the interrupt flag
                                    Thread.currentThread().interrupt();
                                }
                                break;
                            }
                            catch (RetriableException e) {
                                int maxRetries = getErrorsMaxRetries();
                                LOGGER.info("Retriable exception thrown, connector will be restarted; errors.max.retries={}", maxRetries, e);
                                if (maxRetries == 0 || maxRetries < DEFAULT_ERROR_MAX_RETRIES) {
                                    retryError = e;
                                    throw e;
                                }
                                else if (maxRetries != 0) {
                                    DelayStrategy delayStrategy = delayStrategy(config);
                                    int totalRetries = 0;
                                    boolean startedSuccessfully = false;
                                    while (!startedSuccessfully) {
                                        try {
                                            totalRetries++;
                                            LOGGER.info("Starting connector, attempt {}", totalRetries);
                                            task.stop();
                                            task.start(taskConfigs.get(0));
                                            startedSuccessfully = true;
                                        }
                                        catch (Exception ex) {
                                            if (totalRetries == maxRetries) {
                                                LOGGER.error("Can't start the connector, max retries to connect exceeded; stopping connector...", ex);
                                                retryError = ex;
                                                throw ex;
                                            }
                                            else {
                                                LOGGER.error("Can't start the connector, will retry later...", ex);
                                            }
                                        }
                                        delayStrategy.sleepWhen(!startedSuccessfully);
                                    }
                                }
                            }
                            try {
                                if (changeRecords != null && !changeRecords.isEmpty()) {
                                    LOGGER.debug("Received {} records from the task", changeRecords.size());
                                    changeRecords = changeRecords.stream()
                                            .map(transformations::transform)
                                            .filter(x -> x != null)
                                            .collect(Collectors.toList());
                                }

                                if (changeRecords != null && !changeRecords.isEmpty()) {
                                    LOGGER.debug("Received {} transformed records from the task", changeRecords.size());

                                    try {
                                        handler.handleBatch(changeRecords, committer);
                                    }
                                    catch (StopConnectorException e) {
                                        break;
                                    }
                                }
                                else {
                                    LOGGER.debug("Received no records from the task");
                                }
                            }
                            catch (Throwable t) {
                                // There was some sort of unexpected exception, so we should stop work
                                handlerError = t;
                                break;
                            }
                        }
                    }
                    finally {
                        if (handlerError != null) {
                            // There was an error in the handler so make sure it's always captured...
                            fail("Stopping connector after error in the application's handler method: " + handlerError.getMessage(),
                                    handlerError);
                        }
                        else if (retryError != null) {
                            fail("Stopping connector after retry error: " + retryError.getMessage(), retryError);
                        }
                        else {
                            // We stopped normally ...
                            succeed("Connector '" + connectorClassName + "' completed normally.");
                        }
                        try {
                            // First stop the task ...
                            LOGGER.info("Stopping the task and engine");
                            task.stop();
                            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStopped);
                            // Always commit offsets that were captured from the source records we actually processed ...
                            commitOffsets(offsetWriter, commitTimeout, task);
                        }
                        catch (InterruptedException e) {
                            LOGGER.debug("Interrupted while committing offsets");
                            Thread.currentThread().interrupt();
                        }
                        catch (Throwable t) {
                            fail("Error while trying to stop the task and commit the offsets", t);
                        }
                    }
                }
                catch (Throwable t) {
                    fail("Error while trying to run connector class '" + connectorClassName + "'", t);
                }
                finally {
                    // Close the offset storage and finally the connector ...
                    try {
                        offsetStore.stop();
                    }
                    catch (Throwable t) {
                        fail("Error while trying to stop the offset store", t);
                    }
                    finally {
                        try {
                            connector.stop();
                            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStopped);
                        }
                        catch (Throwable t) {
                            fail("Error while trying to stop connector class '" + connectorClassName + "'", t);
                        }
                    }
                }
            }
            finally {
                latch.countDown();
                runningThread.set(null);
                // after we've "shut down" the engine, fire the completion callback based on the results we collected
                completionCallback.handle(completionResult.success(), completionResult.message(), completionResult.error());
            }
        }
    }

    private int getErrorsMaxRetries() {
        int maxRetries = config.getInteger(ERRORS_MAX_RETRIES);
        return maxRetries;
    }

    /**
     * Creates a new RecordCommitter that is responsible for informing the engine
     * about the updates to the given batch
     * @param offsetWriter the offsetWriter current in use
     * @param task the sourcetask
     * @param commitTimeout the time in ms until a commit times out
     * @return the new recordCommitter to be used for a given batch
     */
    protected RecordCommitter buildRecordCommitter(OffsetStorageWriter offsetWriter, SourceTask task, Duration commitTimeout) {
        return new RecordCommitter() {

            @Override
            public synchronized void markProcessed(SourceRecord record) throws InterruptedException {
                task.commitRecord(record);
                recordsSinceLastCommit += 1;
                offsetWriter.offset((Map<String, Object>) record.sourcePartition(), (Map<String, Object>) record.sourceOffset());
            }

            @Override
            public synchronized void markBatchFinished() throws InterruptedException {
                maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeout, task);
            }

            @Override
            public synchronized void markProcessed(SourceRecord record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
                SourceRecordOffsets offsets = (SourceRecordOffsets) sourceOffsets;
                SourceRecord recordWithUpdatedOffsets = new SourceRecord(record.sourcePartition(), offsets.getOffsets(), record.topic(),
                        record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(),
                        record.timestamp(), record.headers());
                markProcessed(recordWithUpdatedOffsets);
            }

            @Override
            public DebeziumEngine.Offsets buildOffsets() {
                return new SourceRecordOffsets();
            }
        };
    }

    /**
     * Implementation of {@link DebeziumEngine.Offsets} which can be used to construct a {@link SourceRecord}
     * with its offsets.
     */
    protected class SourceRecordOffsets implements DebeziumEngine.Offsets {
        private HashMap<String, Object> offsets = new HashMap<>();

        /**
         * Performs {@link HashMap#put(Object, Object)} on the offsets map.
         *
         * @param key key with which to put the value
         * @param value value to be put with the key
         */
        @Override
        public void set(String key, Object value) {
            offsets.put(key, value);
        }

        /**
         * Retrieves the offsets map.
         *
         * @return HashMap of the offsets
         */
        protected HashMap<String, Object> getOffsets() {
            return offsets;
        }
    }

    /**
     * Determine if we should flush offsets to storage, and if so then attempt to flush offsets.
     *
     * @param offsetWriter the offset storage writer; may not be null
     * @param policy the offset commit policy; may not be null
     * @param commitTimeout the timeout to wait for commit results
     * @param task the task which produced the records for which the offsets have been committed
     */
    protected void maybeFlush(OffsetStorageWriter offsetWriter, OffsetCommitPolicy policy, Duration commitTimeout,
                              SourceTask task)
            throws InterruptedException {
        // Determine if we need to commit to offset storage ...
        long timeSinceLastCommitMillis = clock.currentTimeInMillis() - timeOfLastCommitMillis;
        if (policy.performCommit(recordsSinceLastCommit, Duration.ofMillis(timeSinceLastCommitMillis))) {
            commitOffsets(offsetWriter, commitTimeout, task);
        }
    }

    /**
     * Flush offsets to storage.
     *
     * @param offsetWriter the offset storage writer; may not be null
     * @param commitTimeout the timeout to wait for commit results
     * @param task the task which produced the records for which the offsets have been committed
     */
    protected void commitOffsets(OffsetStorageWriter offsetWriter, Duration commitTimeout, SourceTask task) throws InterruptedException {
        long started = clock.currentTimeInMillis();
        long timeout = started + commitTimeout.toMillis();
        if (!offsetWriter.beginFlush()) {
            return;
        }
        Future<Void> flush = offsetWriter.doFlush(this::completedFlush);
        if (flush == null) {
            return; // no offsets to commit ...
        }

        // Wait until the offsets are flushed ...
        try {
            flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
            // if we've gotten this far, the offsets have been committed so notify the task
            task.commit();
            recordsSinceLastCommit = 0;
            timeOfLastCommitMillis = clock.currentTimeInMillis();
        }
        catch (InterruptedException e) {
            LOGGER.warn("Flush of {} offsets interrupted, cancelling", this);
            offsetWriter.cancelFlush();

            if (this.runningThread.get() == Thread.currentThread()) {
                // this thread is still set as the running thread -> we were not interrupted
                // due the stop() call -> probably someone else called the interrupt on us ->
                // -> we should raise the interrupt flag
                Thread.currentThread().interrupt();
                throw e;
            }
        }
        catch (ExecutionException e) {
            LOGGER.error("Flush of {} offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
        }
        catch (TimeoutException e) {
            LOGGER.error("Timed out waiting to flush {} offsets to storage", this);
            offsetWriter.cancelFlush();
        }
    }

    protected void completedFlush(Throwable error, Void result) {
        if (error != null) {
            LOGGER.error("Failed to flush {} offsets to storage: ", this, error);
        }
        else {
            LOGGER.trace("Finished flushing {} offsets to storage", this);
        }
    }

    /**
     * Stop the execution of this embedded connector. This method does not block until the connector is stopped; use
     * {@link #await(long, TimeUnit)} for this purpose.
     *
     * @return {@code true} if the connector was {@link #run() running} and will eventually stop, or {@code false} if it was not
     *         running when this method is called
     * @see #await(long, TimeUnit)
     */
    public boolean stop() {
        LOGGER.info("Stopping the embedded engine");
        // Signal that the run() method should stop ...
        Thread thread = this.runningThread.getAndSet(null);
        if (thread != null) {
            try {
                // Making sure the event source coordinator has enough time to shut down before forcefully stopping it
                Duration timeout = Duration.ofMillis(config.getLong(WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS));
                LOGGER.info("Waiting for {} for connector to stop", timeout);
                latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
            }
            LOGGER.debug("Interrupting the embedded engine's thread {} (already interrupted: {})", thread, thread.isInterrupted());
            // Interrupt the thread in case it is blocked while polling the task for records ...
            thread.interrupt();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    /**
     * Wait for the connector to complete processing. If the processor is not running, this method returns immediately; however,
     * if the processor is {@link #stop() stopped} and restarted before this method is called, this method will return only
     * when it completes the second time.
     *
     * @param timeout the maximum amount of time to wait before returning
     * @param unit the unit of time; may not be null
     * @return {@code true} if the connector completed within the timeout (or was not running), or {@code false} if it is still
     *         running when the timeout occurred
     * @throws InterruptedException if this thread is interrupted while waiting for the completion of the connector
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    @Override
    public String toString() {
        return "EmbeddedEngine{id=" + config.getString(ENGINE_NAME) + '}';
    }

    public void runWithTask(Consumer<SourceTask> consumer) {
        consumer.accept(task);
    }

    private DelayStrategy delayStrategy(Configuration config) {
        return DelayStrategy.exponential(Duration.ofMillis(config.getInteger(ERRORS_RETRY_DELAY_INITIAL_MS)),
                Duration.ofMillis(config.getInteger(ERRORS_RETRY_DELAY_MAX_MS)));
    }

    protected static class EmbeddedConfig extends WorkerConfig {
        private static final ConfigDef CONFIG;

        static {
            ConfigDef config = baseConfigDef();
            Field.group(config, "file", OFFSET_STORAGE_FILE_FILENAME);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_TOPIC);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_PARTITIONS);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR);
            CONFIG = config;
        }

        protected EmbeddedConfig(Map<String, String> props) {
            super(CONFIG, props);
        }
    }
}

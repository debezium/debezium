/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.reactive;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.EmbeddedEngineChangeEventFactory;
import io.debezium.engine.RecordCommitter;
import io.debezium.engine.reactive.CommittableRecord;
import io.debezium.engine.reactive.ReactiveDebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;
import io.debezium.util.VariableLatch;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A mechanism for creating a {@link Publisher} that emits elements.
 * <p>
 * With this connector, it's possible to use the Reactive Streams specification in order to use Debezium as a
 * reactive non-blocking record emitter.
 * <p>
 *
 * @author Gabriel Francisco
 */
@ThreadSafe
public final class EmbeddedReactiveEngine<R> implements ReactiveDebeziumEngine<R> {
    /**
     * A required field for an embedded connector that specifies the unique name for the connector instance.
     */
    public static final Field ENGINE_NAME = Field.create("name")
            .withDescription("Unique name for this connector instance.")
            .withValidation(Field::isRequired);

    /**
     * A required field for an embedded connector that specifies the name of the normal Debezium connector's Java class.
     */
    public static final Field CONNECTOR_CLASS = Field.create("connector.class")
            .withDescription("The Java class for the connector")
            .withValidation(Field::isRequired);

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
            .withType(Type.INT)
            .withDescription("The number of partitions used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional field that specifies the replication factor for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR = Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .withType(Type.SHORT)
            .withDescription("Replication factor used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withDescription("Interval at which to try committing offsets. The default is 1 minute.")
            .withDefault(60000L)
            .withValidation(Field::isNonNegativeInteger);

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    public static final Field OFFSET_COMMIT_TIMEOUT_MS = Field.create("offset.flush.timeout.ms")
            .withDescription("Maximum number of milliseconds to wait for records to flush and partition offset data to be"
                    + " committed to offset storage before cancelling the process and restoring the offset "
                    + "data to be committed in a future attempt.")
            .withDefault(5000L)
            .withValidation(Field::isPositiveInteger);

    public static final Field OFFSET_COMMIT_POLICY = Field.create("offset.commit.policy")
            .withDescription("The fully-qualified class name of the commit policy type. This class must implement the interface "
                    + OffsetCommitPolicy.class.getName()
                    + ". The default is a periodic commit policy based upon time intervals.")
            .withDefault(io.debezium.embedded.spi.OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName())
            .withValidation(Field::isClassName);

    protected static final Field INTERNAL_KEY_CONVERTER_CLASS = Field.create("internal.key.converter")
            .withDescription("The Converter class that should be used to serialize and deserialize key data for offsets.")
            .withDefault(JsonConverter.class.getName());

    protected static final Field INTERNAL_VALUE_CONVERTER_CLASS = Field.create("internal.value.converter")
            .withDescription("The Converter class that should be used to serialize and deserialize value data for offsets.")
            .withDefault(JsonConverter.class.getName());

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

    /**
     * The array of fields that are required by each connectors.
     */
    public static final Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

    /**
     * The array of all exposed fields.
     */
    protected static final Field.Set ALL_FIELDS = CONNECTOR_FIELDS.with(OFFSET_STORAGE, OFFSET_STORAGE_FILE_FILENAME,
            OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS,
            INTERNAL_KEY_CONVERTER_CLASS, INTERNAL_VALUE_CONVERTER_CLASS);

    private static final Duration WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_DEFAULT = Duration.ofSeconds(2);
    private static final String WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_PROP = "debezium.embedded.shutdown.pause.before.interrupt.ms";

    @Override
    public Publisher<CommittableRecord<R>> publisher() {
        final RecordCommitter<R> committer = getCommitter(getOffsetStorageWriter(getOffsetBackingStore()), getCommitTimeout());

        return Flowable.create(
            new DebeziumFlowableOnSubscribe<R>(1, this::poll, committer),
            BackpressureStrategy.BUFFER
        );
    }

    public static final class BuilderImpl<R> implements Builder<R> {
        private Configuration config;
        private ClassLoader classLoader;
        private Clock clock;
        private ReactiveDebeziumEngine.CompletionCallback completionCallback;
        private ReactiveDebeziumEngine.ConnectorCallback connectorCallback;
        private OffsetCommitPolicy offsetCommitPolicy = null;

        @Override
        public Builder<R> using(Properties config) {
            this.config = Configuration.from(config);
            return this;
        }

        @Override
        public Builder<R> using(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder<R> using(Clock clock) {
            this.clock = clock;
            return this;
        }

        @Override
        public Builder<R> using(OffsetCommitPolicy offsetCommitPolicy) {
            this.offsetCommitPolicy = offsetCommitPolicy;
            return this;
        }

        @Override
        public Builder<R> using(java.time.Clock clock) {
            return using(clock::millis);
        }

        @Override
        public Builder<R> using(CompletionCallback completionCallback) {
            this.completionCallback = completionCallback;
            return this;
        }

        @Override
        public Builder<R> using(ConnectorCallback connectorCallback) {
            this.connectorCallback = connectorCallback;
            return this;
        }

        @Override
        public EmbeddedReactiveEngine<R> build() {
            if (classLoader == null) {
                classLoader = getClass().getClassLoader();
            }
            if (clock == null) {
                clock = Clock.system();
            }
            Objects.requireNonNull(config, "A connector configuration must be specified.");
            return new EmbeddedReactiveEngine<R>(config, classLoader, clock, completionCallback, connectorCallback, offsetCommitPolicy);
        }
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
         * @param unit    the time unit of the {@code timeout} argument
         * @return {@code true} if the completion was received, or {@code false} if the waiting time elapsed before the completion
         * was received.
         * @throws InterruptedException if the current thread is interrupted while waiting
         */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.completed.await(timeout, unit);
        }

        /**
         * Determine if the connector has completed.
         *
         * @return {@code true} if the connector has completed, or {@code false} if the connector is still running and this
         * callback has not yet been {@link #handle(boolean, String, Throwable) notified}
         */
        public boolean hasCompleted() {
            return completed.getCount() == 0;
        }

        /**
         * Get whether the connector completed normally.
         *
         * @return {@code true} if the connector completed normally, or {@code false} if the connector produced an error that
         * prevented startup or premature termination (or the connector has not yet {@link #hasCompleted() completed})
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
         * the connector has not yet {@link #hasCompleted() completed}
         */
        public boolean hasError() {
            return error != null;
        }
    }

    /**
     * Obtain a new {@link Builder} instance that can be used to construct runnable {@link EmbeddedReactiveEngine} instances.
     *
     * @return the new builder; never null
     */
    @Deprecated
    public static Builder create() {
        return new BuilderImpl();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedReactiveEngine.class);

    private final Configuration config;
    private final Clock clock;
    private final ClassLoader classLoader;
    private final ReactiveDebeziumEngine.CompletionCallback completionCallback;
    private final ReactiveDebeziumEngine.ConnectorCallback connectorCallback;
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

    private EmbeddedReactiveEngine(Configuration config, ClassLoader classLoader, Clock clock,
                                   ReactiveDebeziumEngine.CompletionCallback completionCallback,
                                   ReactiveDebeziumEngine.ConnectorCallback connectorCallback,
                                   OffsetCommitPolicy offsetCommitPolicy) {
        this.config = config;
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
        assert this.classLoader != null;
        assert this.clock != null;
        keyConverter = config.getInstance(INTERNAL_KEY_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
        keyConverter.configure(config.subset(INTERNAL_KEY_CONVERTER_CLASS.name() + ".", true).asMap(), true);
        valueConverter = config.getInstance(INTERNAL_VALUE_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
        Configuration valueConverterConfig = config;
        if (valueConverter instanceof JsonConverter) {
            // Make sure that the JSON converter is configured to NOT enable schemas ...
            valueConverterConfig = config.edit().with(INTERNAL_VALUE_CONVERTER_CLASS + ".schemas.enable", false).build();
        }
        valueConverter.configure(valueConverterConfig.subset(INTERNAL_VALUE_CONVERTER_CLASS.name() + ".", true).asMap(), false);

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

    public void start() {
        if (runningThread.compareAndSet(null, Thread.currentThread())) {
            final String engineName = getEngineName();
            final String connectorClassName = getConnectorClass();
            final Optional<ReactiveDebeziumEngine.ConnectorCallback> connectorCallback = Optional.ofNullable(this.connectorCallback);
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
                } catch (Throwable t) {
                    fail("Unable to instantiate connector class '" + connectorClassName + "'", t);
                    return;
                }

                // Instantiate the offset store ...
                OffsetBackingStore offsetStore = getOffsetBackingStore();
                if (offsetStore == null) return;

                // Set up the offset commit policy ...
                offsetPolicy();

                // Initialize the connector using a context that does NOT respond to requests to reconfigure tasks ...
                ConnectorContext context = new ConnectorContext() {

                    @Override
                    public void requestTaskReconfiguration() {
                        // Do nothing ...
                    }

                    @Override
                    public void raiseError(Exception e) {
                        fail(e.getMessage(), e);
                    }

                };
                connector.initialize(context);
                OffsetStorageWriter offsetWriter = getOffsetStorageWriter(offsetStore);
                OffsetStorageReader offsetReader = getOffsetReader(offsetStore);

                Duration commitTimeout = getCommitTimeout();

                try {
                    // Start the connector with the given properties and get the task configurations ...
                    connector.start(config.asMap());
                    connectorCallback.ifPresent(ReactiveDebeziumEngine.ConnectorCallback::connectorStarted);
                    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
                    Class<? extends Task> taskClass = connector.taskClass();
                    task = null;
                    try {
                        task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
                    } catch (IllegalAccessException | InstantiationException t) {
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
                        connectorCallback.ifPresent(ReactiveDebeziumEngine.ConnectorCallback::taskStarted);
                    } catch (Throwable t) {
                        // Mask the passwords ...
                        Configuration config = Configuration.from(taskConfigs.get(0)).withMaskedPasswords();
                        String msg = "Unable to initialize and start connector's task class '" + taskClass.getName() + "' with config: "
                                + config;
                        fail(msg, t);
                        return;
                    }

                    recordsSinceLastCommit = 0;
                    Throwable handlerError = null;
                    try {
                        timeOfLastCommitMillis = clock.currentTimeInMillis();
                        final RecordCommitter<R> committer = getCommitter(offsetWriter, commitTimeout);
                        while (runningThread.get() != null) {
                            List<SourceRecord> changeRecords = null;
                            try {
                                LOGGER.debug("Embedded engine is polling task for records on thread {}", runningThread.get());
                                changeRecords = task.poll(); // blocks until there are values ...
                                LOGGER.debug("Embedded engine returned from polling task for records");
                            } catch (InterruptedException e) {
                                // Interrupted while polling ...
                                LOGGER.debug("Embedded engine interrupted on thread {} while polling the task for records", runningThread.get());
                                if (this.runningThread.get() == Thread.currentThread()) {
                                    // this thread is still set as the running thread -> we were not interrupted
                                    // due the stop() call -> probably someone else called the interrupt on us ->
                                    // -> we should raise the interrupt flag
                                    Thread.currentThread().interrupt();
                                }
                                break;
                            } catch (RetriableException e) {
                                LOGGER.info("Retrieable exception thrown, connector will be restarted", e);
                                // Retriable exception should be ignored by the engine
                                // and no change records delivered.
                                // The retry is handled in io.debezium.connector.common.BaseSourceTask.poll()
                            }
                        }
                    } finally {
                        if (handlerError != null) {
                            // There was an error in the handler so make sure it's always captured...
                            fail("Stopping connector after error in the application's handler method: " + handlerError.getMessage(),
                                    handlerError);
                        }
                        try {
                            // First stop the task ...
                            LOGGER.debug("Stopping the task and engine");
                            task.stop();
                            connectorCallback.ifPresent(ReactiveDebeziumEngine.ConnectorCallback::taskStopped);
                            // Always commit offsets that were captured from the source records we actually processed ...
                            commitOffsets(offsetWriter, commitTimeout, task);
                            if (handlerError == null) {
                                // We stopped normally ...
                                succeed("Connector '" + connectorClassName + "' completed normally.");
                            }
                        } catch (Throwable t) {
                            fail("Error while trying to stop the task and commit the offsets", t);
                        }
                    }
                } catch (Throwable t) {
                    fail("Error while trying to run connector class '" + connectorClassName + "'", t);
                } finally {
                    // Close the offset storage and finally the connector ...
                    try {
                        offsetStore.stop();
                    } catch (Throwable t) {
                        fail("Error while trying to stop the offset store", t);
                    } finally {
                        try {
                            connector.stop();
                            connectorCallback.ifPresent(ReactiveDebeziumEngine.ConnectorCallback::connectorStopped);
                        } catch (Throwable t) {
                            fail("Error while trying to stop connector class '" + connectorClassName + "'", t);
                        }
                    }
                }
            } finally {
                latch.countDown();
                runningThread.set(null);
                // after we've "shut down" the engine, fire the completion callback based on the results we collected
                completionCallback.handle(completionResult.success(), completionResult.message(), completionResult.error());
            }
        }
    }

    private Duration getCommitTimeout() {
        return Duration.ofMillis(config.getLong(OFFSET_COMMIT_TIMEOUT_MS));
    }

    private String getConnectorClass() {
        return config.getString(CONNECTOR_CLASS);
    }

    private String getEngineName() {
        return config.getString(ENGINE_NAME);
    }

    private RecordCommitter<R> getCommitter(OffsetStorageWriter offsetWriter, Duration commitTimeout) {
        return (RecordCommitter<R>) buildRecordCommitter(offsetWriter, task, commitTimeout);
    }

    private OffsetStorageReaderImpl getOffsetReader(OffsetBackingStore offsetStore) {
        return new OffsetStorageReaderImpl(offsetStore, getEngineName(),
                keyConverter, valueConverter);
    }

    private OffsetStorageWriter getOffsetStorageWriter(OffsetBackingStore offsetStore) {
        return new OffsetStorageWriter(offsetStore, getEngineName(),
                            keyConverter, valueConverter);
    }

    private void offsetPolicy() {
        if (offsetCommitPolicy == null) {
            offsetCommitPolicy = config.getInstance(EmbeddedReactiveEngine.OFFSET_COMMIT_POLICY, OffsetCommitPolicy.class, config);
        }
    }

    private OffsetBackingStore getOffsetBackingStore() {
        final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
        OffsetBackingStore offsetStore = null;
        try {
            @SuppressWarnings("unchecked")
            Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
            offsetStore = offsetStoreClass.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            fail("Unable to instantiate OffsetBackingStore class '" + offsetStoreClassName + "'", t);
            return null;
        }

        // Initialize the offset store ...
        try {
            offsetStore.configure(workerConfig);
            offsetStore.start();
        } catch (Throwable t) {
            fail("Unable to configure and start the '" + offsetStoreClassName + "' offset backing store", t);
            return null;
        }
        return offsetStore;
    }

    private List<R> poll() throws InterruptedException {
        final List<SourceRecord> sourceRecords = task.poll();

        return sourceRecords
                .stream()
                .filter(Objects::nonNull)
                .map(s ->
                        (R) EmbeddedEngineChangeEventFactory
                                .from(null,
                                        keyConverter,
                                        valueConverter,
                                        s))
                .collect(Collectors.toList());
    }

    /**
     * Creates a new RecordCommitter that is responsible for informing the engine
     * about the updates to the given batch
     *
     * @param offsetWriter  the offsetWriter current in use
     * @param task          the sourcetask
     * @param commitTimeout the time in ms until a commit times out
     * @return the new recordCommitter to be used for a given batch
     */
    protected RecordCommitter<EmbeddedEngineChangeEvent<?, ?>> buildRecordCommitter(OffsetStorageWriter offsetWriter, SourceTask task, Duration commitTimeout) {
        return new RecordCommitter<EmbeddedEngineChangeEvent<?, ?>>() {
            @Override
            public void markProcessed(EmbeddedEngineChangeEvent<?, ?> event) throws InterruptedException {
                final SourceRecord record = event.sourceRecord();

                task.commitRecord(record);
                recordsSinceLastCommit += 1;
                offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
            }

            @Override
            public synchronized void markBatchFinished() {
                maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeout, task);
            }
        };
    }

    /**
     * Determine if we should flush offsets to storage, and if so then attempt to flush offsets.
     *
     * @param offsetWriter  the offset storage writer; may not be null
     * @param policy        the offset commit policy; may not be null
     * @param commitTimeout the timeout to wait for commit results
     * @param task          the task which produced the records for which the offsets have been committed
     */
    protected void maybeFlush(OffsetStorageWriter offsetWriter, OffsetCommitPolicy policy, Duration commitTimeout,
                              SourceTask task) {
        // Determine if we need to commit to offset storage ...
        long timeSinceLastCommitMillis = clock.currentTimeInMillis() - timeOfLastCommitMillis;
        if (policy.performCommit(recordsSinceLastCommit, Duration.ofMillis(timeSinceLastCommitMillis))) {
            commitOffsets(offsetWriter, commitTimeout, task);
        }
    }

    /**
     * Flush offsets to storage.
     *
     * @param offsetWriter  the offset storage writer; may not be null
     * @param commitTimeout the timeout to wait for commit results
     * @param task          the task which produced the records for which the offsets have been committed
     */
    protected void commitOffsets(OffsetStorageWriter offsetWriter, Duration commitTimeout, SourceTask task) {
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
        } catch (InterruptedException e) {
            LOGGER.warn("Flush of {} offsets interrupted, cancelling", this);
            offsetWriter.cancelFlush();
        } catch (ExecutionException e) {
            LOGGER.error("Flush of {} offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
        } catch (TimeoutException e) {
            LOGGER.error("Timed out waiting to flush {} offsets to storage", this);
            offsetWriter.cancelFlush();
        }
    }

    protected void completedFlush(Throwable error, Void result) {
        if (error != null) {
            LOGGER.error("Failed to flush {} offsets to storage: ", this, error);
        } else {
            LOGGER.trace("Finished flushing {} offsets to storage", this);
        }
    }

    /**
     * Stop the execution of this embedded connector. This method does not block until the connector is stopped; use
     * {@link #await(long, TimeUnit)} for this purpose.
     *
     * @return {@code true} if the connector was {@link #run() running} and will eventually stop, or {@code false} if it was not
     * running when this method is called
     * @see #await(long, TimeUnit)
     */
    public boolean stop() {
        LOGGER.debug("Stopping the embedded engine");
        // Signal that the run() method should stop ...
        Thread thread = this.runningThread.getAndSet(null);
        if (thread != null) {
            try {
                latch.await(
                        Long.valueOf(
                                System.getProperty(WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_PROP, Long.toString(WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_DEFAULT.toMillis()))),
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
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
     * @param unit    the unit of time; may not be null
     * @return {@code true} if the connector completed within the timeout (or was not running), or {@code false} if it is still
     * running when the timeout occurred
     * @throws InterruptedException if this thread is interrupted while waiting for the completion of the connector
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    @Override
    public String toString() {
        return "EmbeddedEngine{id=" + config.getString(ENGINE_NAME) + '}';
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

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

    public static final Field OFFSET_STORAGE = OffsetManager.OFFSET_STORAGE;
    /**
     * An optional field to specify total number of embedded engines that will
     * be instantiated.
     */
    public static final Field MAX_TASKS = Field.create(ConnectorConfig.TASKS_MAX_CONFIG)
            .withDescription("Total number of tasks instantiated.")
            .withDefault(1);

    /**
     * An optional field that specifies the file location for the {@link FileOffsetBackingStore}.
     *
     * @see OffsetManager#OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_FILE_FILENAME = Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG)
            .withDescription("The file where offsets are to be stored. Required when "
                    + "'offset.storage' is set to the " +
                    FileOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the topic name for the {@link KafkaOffsetBackingStore}.
     *
     * @see OffsetManager#OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_TOPIC = Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)
            .withDescription("The name of the Kafka topic where offsets are to be stored. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the number of partitions for the {@link KafkaOffsetBackingStore}.
     *
     * @see OffsetManager#OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_PARTITIONS = Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG)
            .withType(ConfigDef.Type.INT)
            .withDescription("The number of partitions used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional field that specifies the replication factor for the {@link KafkaOffsetBackingStore}.
     *
     * @see OffsetManager#OFFSET_STORAGE
     */
    public static final Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR = Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .withType(ConfigDef.Type.SHORT)
            .withDescription("Replication factor used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    public static final Field OFFSET_FLUSH_INTERVAL_MS = TaskOffsetManager.OFFSET_FLUSH_INTERVAL_MS;
    public static final Field OFFSET_COMMIT_TIMEOUT_MS = TaskOffsetManager.OFFSET_COMMIT_TIMEOUT_MS;
    public static final Field OFFSET_COMMIT_POLICY = TaskOffsetManager.OFFSET_COMMIT_POLICY;
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

    public static final Field WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS = Field.create("debezium.embedded.shutdown.pause.before.interrupt.ms")
            .withDisplayName("Time to wait to engine completion before interrupt")
            .withType(Type.LONG)
            .withDefault(Duration.ofMinutes(5).toMillis())
            .withValidation(Field::isPositiveInteger)
            .withDescription(String.format("How long we wait before forcefully stopping the connector thread when shutting down. " +
                    "Must be bigger than the time it takes two polling loops to finish ({} ms)", ChangeEventSourceCoordinator.SHUTDOWN_WAIT_TIMEOUT.toMillis() * 2));

    /**
     * The array of fields that are required by each connector.
     */
    public static final Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

    /**
     * The array of all exposed fields.
     */
    static final Field.Set ALL_FIELDS = CONNECTOR_FIELDS.with(OFFSET_STORAGE, OFFSET_STORAGE_FILE_FILENAME,
            OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS,
            TaskWorker.ERRORS_MAX_RETRIES, TaskWorker.ERRORS_RETRY_DELAY_INITIAL_MS, TaskWorker.ERRORS_RETRY_DELAY_MAX_MS);

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

            if (offsetCommitPolicy != null) {
                config = config.edit().with(TaskOffsetManager.OFFSET_COMMIT_POLICY, offsetCommitPolicy.getClass().getName()).build();
            }

            return new EmbeddedEngine(config, classLoader, clock,
                    handler, completionCallback, connectorCallback);
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
    private final VariableLatch latch = new VariableLatch(0);
    private final WorkerConfig workerConfig;
    private final CompletionResult completionResult;
    private OffsetCommitPolicy offsetCommitPolicy;
    private final Transformations transformations;

    private final EmbeddedEngineState embeddedEngineState = new EmbeddedEngineState();

    private final Map<Integer, TaskWorker> taskWorkers = new ConcurrentHashMap<>();

    private EmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, DebeziumEngine.ChangeConsumer<SourceRecord> handler,
                           DebeziumEngine.CompletionCallback completionCallback, DebeziumEngine.ConnectorCallback connectorCallback) {
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
        assert this.config != null;
        assert this.handler != null;
        assert this.classLoader != null;
        assert this.clock != null;

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
        return this.embeddedEngineState.isRunning();
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
        if (this.embeddedEngineState.isStopped()) {
            this.embeddedEngineState.start();
            ExecutorService executorService = Executors.newCachedThreadPool();

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

                OffsetManager offsetManager = new DefaultOffsetManager();
                offsetManager.configure(config);

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
                        return offsetManager.offsetStorageReader();
                    }
                };
                connector.initialize(context);

                int maxTasks = config.getInteger(MAX_TASKS);

                try {
                    // Start the connector with the given properties and get the task configurations ...
                    connector.start(connectorConfig);
                    connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStarted);
                    List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);

                    Class<? extends Task> taskClass = connector.taskClass();
                    if (taskConfigs.isEmpty()) {
                        String msg = "Unable to start connector's task class '" + taskClass.getName() + "' with no task configuration";
                        fail(msg);
                        return;
                    }

                    CountDownLatch waitForCompletion = new CountDownLatch(taskConfigs.size());
                    IntStream.range(0, taskConfigs.size()).forEach(taskId -> {
                        executorService.submit(() -> {
                            try {
                                MDC.put("taskId", String.valueOf(taskId));
                                Map<String, String> taskConfig = taskConfigs.get(taskId);

                                if (maxTasks > 0) {
                                    taskConfig = new HashMap<>(taskConfig);
                                    if (taskConfig.containsKey(OFFSET_STORAGE_FILE_FILENAME.name())) {
                                        taskConfig.replace(OFFSET_STORAGE_FILE_FILENAME.name(),
                                                String.format(connectorConfig.get(OFFSET_STORAGE_FILE_FILENAME.name()), taskId));
                                    }
                                    else if (taskConfig.containsKey(OFFSET_STORAGE_KAFKA_TOPIC.name())) {
                                        taskConfig.replace(OFFSET_STORAGE_KAFKA_TOPIC.name(),
                                                String.format(connectorConfig.get(OFFSET_STORAGE_KAFKA_TOPIC.name()), taskId));
                                    }
                                }

                                TaskWorker taskWorker = new TaskWorker(
                                        taskId, taskClass, embeddedEngineState, handler, transformations, clock, connectorCallback.orElse(null), completionResult);
                                taskWorkers.put(taskId, taskWorker);

                                taskWorker.configure(Configuration.from(taskConfig));
                                taskWorker.run();
                            }
                            catch (Throwable t) {
                                fail("Error while trying to run task class '" + taskClass.getName() + "'", t);
                            }
                            finally {
                                MDC.clear();
                                waitForCompletion.countDown();
                            }
                        });
                    });

                    try {
                        waitForCompletion.await();
                    }
                    catch (InterruptedException e) {
                        if (embeddedEngineState.isRunning()) {
                            // the engine thread was interrupted for an unknown reason...
                        }
                        Thread.currentThread().interrupt();
                    }
                }
                catch (Throwable t) {
                    fail("Error while trying to run connector class '" + connectorClassName + "'", t);
                }
                finally {
                    // Close the offset storage and finally the connector ...
                    try {
                        offsetManager.stop();
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
                this.embeddedEngineState.stop();
                executorService.shutdown();
                // after we've "shut down" the engine, fire the completion callback based on the results we collected
                completionCallback.handle(completionResult.success(), completionResult.message(), completionResult.error());
            }
        }
    }

    /**
     * Creates a new RecordCommitter that is responsible for informing the engine
     * about the updates to the given batch
     * @return the new recordCommitter to be used for a given batch
     */
    static RecordCommitter buildRecordCommitter(TaskOffsetManager taskOffsetManager) {
        return new RecordCommitter() {

            @Override
            public synchronized void markProcessed(SourceRecord record) throws InterruptedException {
                taskOffsetManager.commit(record);
            }

            @Override
            public synchronized void markBatchFinished() throws InterruptedException {
                taskOffsetManager.maybeFlush();
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
    static class SourceRecordOffsets implements DebeziumEngine.Offsets {
        private final HashMap<String, Object> offsets = new HashMap<>();

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
     * Stop the execution of this embedded connector. This method does not block until the connector is stopped; use
     * {@link #await(long, TimeUnit)} for this purpose.
     *
     * @return {@code true} if the connector was {@link #run() running} and will eventually stop, or {@code false} if it was not
     *         running when this method is called
     * @see #await(long, TimeUnit)
     */
    public boolean stop() {
        LOGGER.info("Stopping the embedded engine");
        if (this.embeddedEngineState.isRunning()) {
            Thread thread = this.embeddedEngineState.stop();
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
        taskWorkers.values().forEach(worker -> consumer.accept(worker.task()));
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

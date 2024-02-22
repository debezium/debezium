/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.embedded.ConverterBuilder;
import io.debezium.embedded.DebeziumEngineCommon;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.EmbeddedWorkerConfig;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.embedded.Transformations;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.source.EngineSourceConnector;
import io.debezium.engine.source.EngineSourceConnectorContext;
import io.debezium.engine.source.EngineSourceTask;
import io.debezium.engine.source.EngineSourceTaskContext;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.DelayStrategy;

/**
 * Implementation of {@link DebeziumEngine} which allows to run multiple tasks in parallel and also
 * allows to process part or whole record processing pipeline in parallel.
 * For more detail see DDD-7 (TODO link).
 *
 * @author vjuranek
 */
public final class AsyncEmbeddedEngine<R> implements DebeziumEngine<R>, AsyncEngineConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncEmbeddedEngine.class);

    private final Configuration config;
    private final io.debezium.util.Clock clock;
    private final ClassLoader classLoader;
    private final Consumer<R> consumer;
    private final DebeziumEngine.ChangeConsumer<R> handler;
    private final DebeziumEngine.CompletionCallback completionCallback;
    private final Optional<DebeziumEngine.ConnectorCallback> connectorCallback;
    private final Converter offsetKeyConverter;
    private final Converter offsetValueConverter;
    private final WorkerConfig workerConfig;
    private final OffsetCommitPolicy offsetCommitPolicy;
    private final EngineSourceConnector connector;
    private final Transformations transformations;
    private final HeaderConverter headerConverter;
    private final Function<SourceRecord, R> recordConverter;
    private final Function<R, SourceRecord> sourceConverter;

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATING); // state must be changed only via setEngineState() method
    private final List<EngineSourceTask> tasks = new ArrayList<>();
    private final List<Future<Void>> pollingFutures = new ArrayList<>();
    private final ExecutorService taskService;
    private final ExecutorService recordService;
    // A latch to make sure close() method finishes before we call completion callback, see also DBZ-7496.
    private final CountDownLatch shutDownLatch = new CountDownLatch(1);

    private AsyncEmbeddedEngine(Properties config,
                                Consumer<R> consumer,
                                DebeziumEngine.ChangeConsumer<R> handler,
                                ClassLoader classLoader,
                                io.debezium.util.Clock clock,
                                DebeziumEngine.CompletionCallback completionCallback,
                                DebeziumEngine.ConnectorCallback connectorCallback,
                                OffsetCommitPolicy offsetCommitPolicy,
                                HeaderConverter headerConverter,
                                Function<SourceRecord, R> recordConverter) {

        this.config = Configuration.from(Objects.requireNonNull(config, "A connector configuration must be specified."));
        this.consumer = consumer;
        this.handler = handler;
        this.classLoader = classLoader == null ? Instantiator.getClassLoader() : classLoader;
        this.clock = clock == null ? io.debezium.util.Clock.system() : clock;
        this.completionCallback = completionCallback != null ? completionCallback : new DefaultCompletionCallback();
        this.connectorCallback = Optional.ofNullable(connectorCallback);
        this.headerConverter = headerConverter;
        this.recordConverter = recordConverter;
        this.sourceConverter = (record) -> ((EmbeddedEngineChangeEvent<?, ?, ?>) record).sourceRecord();

        // Ensure either user ChangeConsumer or Consumer is provided and validate supported records ordering is provided when relevant.
        if (this.handler == null & this.consumer == null) {
            throw new DebeziumException("Either java.util.function.Consumer or DebeziumEngine.ChangeConsumer must be specified.");
        }
        if (this.handler == null && RecordProcessingOrder.parse(this.config.getString(AsyncEngineConfig.RECORD_PROCESSING_ORDER)) == null) {
            throw new DebeziumException(
                    String.format("'%s' is not a valid 'record.processing.order' options", this.config.getString(AsyncEngineConfig.RECORD_PROCESSING_ORDER)));
        }

        // Create thread pools for executing tasks and record pipelines.
        taskService = Executors.newFixedThreadPool(this.config.getInteger(ConnectorConfig.TASKS_MAX_CONFIG, () -> 1));
        recordService = Executors.newFixedThreadPool(computeRecordThreads(this.config.getString(AsyncEmbeddedEngine.RECORD_PROCESSING_THREADS)));

        // Validate provided config and prepare Kafka worker config needed for Kafka stuff, like e.g. OffsetStore.
        if (!this.config.validateAndRecord(AsyncEngineConfig.CONNECTOR_FIELDS, LOGGER::error)) {
            DebeziumException e = new DebeziumException("Failed to start connector with invalid configuration (see logs for actual errors)", null);
            this.completionCallback.handle(false, "Failed to start connector with invalid configuration (see logs for actual errors)", e);
            throw e;
        }
        workerConfig = new EmbeddedWorkerConfig(this.config.asMap(AsyncEngineConfig.ALL_FIELDS));

        // Instantiate remaining required objects.
        try {
            this.offsetCommitPolicy = offsetCommitPolicy == null
                    ? Instantiator.getInstanceWithProperties(this.config.getString(AsyncEngineConfig.OFFSET_COMMIT_POLICY), config)
                    : offsetCommitPolicy;
            offsetKeyConverter = Instantiator.getInstance(JsonConverter.class.getName());
            offsetValueConverter = Instantiator.getInstance(JsonConverter.class.getName());
            transformations = new Transformations(Configuration.from(config));

            final Class<? extends SourceConnector> connectorClass = (Class<SourceConnector>) this.classLoader
                    .loadClass(this.config.getString(AsyncEngineConfig.CONNECTOR_CLASS));
            final SourceConnector connectConnector = connectorClass.getDeclaredConstructor().newInstance();
            this.connector = new EngineSourceConnector(connectConnector);
        }
        catch (Throwable t) {
            this.completionCallback.handle(false, "Failed to instantiate required class", t);
            throw new DebeziumException(t);
        }

        // Disable schema for default JSON converters used for offset store.
        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        offsetKeyConverter.configure(internalConverterConfig, true);
        offsetValueConverter.configure(internalConverterConfig, false);
    }

    @Override
    public void run() {
        Throwable exitError = null;
        try {
            LOGGER.debug("Initializing connector and starting it.");
            setEngineState(State.CREATING, State.INITIALIZING);
            connector.connectConnector().start(initializeConnector());
            LOGGER.debug("Calling connector callback after connector has started.");
            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStarted);

            LOGGER.debug("Creating source tasks.");
            setEngineState(State.INITIALIZING, State.CREATING_TASKS);
            createSourceTasks(connector, tasks);

            LOGGER.debug("Starting source tasks.");
            setEngineState(State.CREATING_TASKS, State.STARTING_TASKS);
            startSourceTasks(tasks);

            LOGGER.debug("Starting tasks polling.");
            setEngineState(State.STARTING_TASKS, State.POLLING_TASKS);
            runTasksPolling(tasks);
            // Tasks run infinite polling loop until close() is called or exception is thrown.
        }
        catch (Throwable t) {
            exitError = t;
            closeEngineWithException(exitError);
        }
        finally {
            finishShutDown(exitError);
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.debug("Engine shutdown called.");
        // Actual engine state may change until we pass all the checks, but in such case we fail in setEngineState() method.
        final State engineState = getEngineState();

        // Stopping the engine is not allowed from State.STARING_TASKS as we typically open connections during this phase and shutdown in another thread
        // may result in leaked connections and/or other unwanted side effects.
        // If the state in `engineState` is State.STARTING_CONNECTOR and the state has changed to State.STARING_TASKS now, we fail in setEngineState() right after
        // these checks, so we fail before calling actual engine shutdown.
        // Vice versa, if the state is State.STARTING_CONNECTOR, and we succeeded with setting the state to State.STOPPING, we eventually fail to set state before
        // calling startSourceTasks() as the state is not State.STARTING_CONNECTOR anymore.
        // See https://issues.redhat.com/browse/DBZ-2534 for more details.
        if (engineState == State.STARTING_TASKS) {
            throw new IllegalStateException("Cannot stop engine while tasks are starting, this may lead to leaked resource. Wait for the tasks to be fully started.");
        }

        // Stopping the tasks should be idempotent, but who knows, better to avoid situation when stop is called multiple times.
        if (engineState == State.STOPPING) {
            throw new IllegalStateException("Engine is already being shutting down.");
        }

        // Stopping already stopped engine very likely signals an error in the code using Debezium engine.
        // Moreover, doing any operations with already stopped engine is forbidden.
        if (engineState == State.STOPPED) {
            throw new IllegalStateException("Engine has been already shut down.");
        }

        LOGGER.debug("Stopping " + AsyncEmbeddedEngine.class.getName());
        // Engine state must not change during the checks above and has to be the same as the one stored in the `engineState`.
        setEngineState(engineState, State.STOPPING);
        close(engineState);
    }

    /**
     * For backward compatibility with tests and for testing purposes ONLY. MUST NOT be used in user applications!
     * Exposes tasks to a use defined consumer, which allows to run the tasks in tests.
     *
     * @param consumer {@link Consumer} for running tasks.
     */
    @VisibleForTesting
    public void runWithTask(final Consumer<SourceTask> consumer) {
        for (EngineSourceTask task : tasks) {
            consumer.accept(task.connectTask());
        }
    }

    /**
     * Shuts down the engine. Currently, it's limited only to closing header converter and stopping the source connector.
     *
     * @param stateBeforeStop {@link State} of the engine when the shutdown was requested.
     */
    private void close(final State stateBeforeStop) {
        if (headerConverter != null) {
            try {
                headerConverter.close();
            }
            catch (IOException e) {
                LOGGER.warn("Failed to close header converter: ", e);
            }
        }
        stopConnector(tasks, stateBeforeStop);
        shutDownLatch.countDown();
    }

    /**
     * Stops engine if needed upon the failure. The engine is stopped if it's not already stopped, or it's being stopped in right now.
     *
     * @param exitError {@link Throwable} which was thrown during the engine run and propagated to the main thread.
     */
    private void closeEngineWithException(Throwable exitError) {
        LOGGER.error("Engine has failed with ", exitError);

        final State stateBeforeStop = getEngineState();
        // Skip shutting down the engine if it's already being stopped.
        if (State.canBeStopped(stateBeforeStop)) {
            LOGGER.debug("Stopping " + AsyncEmbeddedEngine.class.getName());
            setEngineState(stateBeforeStop, State.STOPPING);
            try {
                close(stateBeforeStop);
            }
            catch (Throwable ct) {
                LOGGER.error("Failed to close the engine: ", ct);
            }
        }
    }

    /**
     * Finish engine shut down - move it into {@code STOPPED} state and call the completion callback.
     *
     * @param exitError {@link Throwable} which was thrown during engine run, {@code null} is engine finished without any issue.
     */
    private void finishShutDown(Throwable exitError) {
        try {
            shutDownLatch.await();
        }
        catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for shutdown to finish.");
        }
        LOGGER.info("Engine is stopped.");
        setEngineState(State.STOPPING, State.STOPPED);
        LOGGER.debug("Calling completion handler.");
        callCompletionHandler(exitError);
    }

    /**
     * Initialize all the required pieces for initialization of the connector and returns configuration of the connector.
     *
     * @return {@link Map<String, String>} with connector configuration.
     */
    private Map<String, String> initializeConnector() throws Exception {
        LOGGER.debug("Preparing connector initialization");
        final String engineName = config.getString(AsyncEngineConfig.ENGINE_NAME);
        final String connectorClassName = config.getString(AsyncEngineConfig.CONNECTOR_CLASS);
        final Map<String, String> connectorConfig = validateAndGetConnectorConfig(connector.connectConnector(), connectorClassName);

        LOGGER.debug("Initializing offset store, offset reader and writer");
        final OffsetBackingStore offsetStore = createAndStartOffsetStore(connectorConfig);
        final OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName, offsetKeyConverter, offsetValueConverter);
        final OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName, offsetKeyConverter, offsetValueConverter);

        LOGGER.debug("Initializing Connect connector itself");
        connector.initialize(new EngineSourceConnectorContext(this, offsetReader, offsetWriter));

        return connectorConfig;
    }

    /**
     * Creates list of connector tasks to be started as the sources of records.
     *
     * @param connector {@link EngineSourceConnector} to which the source tasks belong to.
     * @param tasks {@link List<EngineSourceTask>} to be populated by the source tasks create in this method.
     */
    private void createSourceTasks(final EngineSourceConnector connector, final List<EngineSourceTask> tasks)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        final Class<? extends Task> taskClass = connector.connectConnector().taskClass();
        final List<Map<String, String>> taskConfigs = connector.connectConnector().taskConfigs(config.getInteger(ConnectorConfig.TASKS_MAX_CONFIG, 1));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Following task configurations will be used for creating tasks:");
            for (int i = 0; i < taskConfigs.size(); i++) {
                LOGGER.debug("Config #{}: {}", i, taskConfigs.get(i));
            }
        }

        if (taskConfigs.size() < 1) {
            LOGGER.warn("No task configuration provided.");
        }
        else {
            LOGGER.debug("Creating {} instance(s) of source task(s)", taskConfigs.size());
        }
        for (Map<String, String> taskConfig : taskConfigs) {
            final SourceTask task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
            final EngineSourceTaskContext taskContext = new EngineSourceTaskContext(
                    taskConfig,
                    connector.context().offsetStorageReader(),
                    connector.context().offsetStorageWriter(),
                    offsetCommitPolicy,
                    clock,
                    transformations);
            task.initialize(taskContext); // Initialize Kafka Connect source task
            tasks.add(new EngineSourceTask(task, taskContext)); // Create new DebeziumSourceTask
        }
    }

    /**
     * Starts the source tasks.
     * The caller is responsible for handling possible error states.
     * However, all the tasks are awaited to either start of fail.
     *
     * @param tasks {@link List<EngineSourceTask>} of tasks to be started
     */
    private void startSourceTasks(final List<EngineSourceTask> tasks) throws Exception {
        LOGGER.debug("Starting source connector tasks.");
        final ExecutorCompletionService<Void> taskCompletionService = new ExecutorCompletionService(taskService);
        for (EngineSourceTask task : tasks) {
            taskCompletionService.submit(() -> {
                task.connectTask().start(task.context().config());
                return null;
            });
        }

        final long taskStartupTimeout = config.getLong(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS);
        LOGGER.debug("Waiting max. for {} ms for individual source tasks to start.", taskStartupTimeout);
        final int nTasks = tasks.size();
        Exception error = null;
        int failedTasks = 0;
        // To avoid leaked resources, we have to ensure that all tasks that were scheduled to start are really started before we continue with the execution in
        // the main (engine) thread and change engine state. If any of the scheduled tasks has failed, catch the exception, wait for other tasks to start and then
        // re-throw the exception and let engine stop already running tasks gracefully during the engine shutdown.
        for (int i = 0; i < nTasks; i++) {
            try {
                final Future<Void> taskFuture = taskCompletionService.poll(taskStartupTimeout, TimeUnit.MILLISECONDS);
                if (taskFuture != null) {
                    taskFuture.get(); // we need to retrieve the results to propagate eventual exceptions
                }
                else {
                    throw new InterruptedException("Time out while waiting for source task to start.");
                }

                LOGGER.debug("Started task #{} out of {} tasks.", i + 1, nTasks);
            }
            catch (Exception e) {
                LOGGER.debug("Task #{} (out of {} tasks) failed to start. Failed with", i + 1, nTasks, e);
                failedTasks++;

                // Store only the first error.
                if (error == null) {
                    error = e;
                }
                continue;
            }
            LOGGER.debug("Calling connector callback after task is started.");
            // TODO improve Debezium API and provide more info to the callback like id and config
            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStarted);
        }

        // If at least one task failed to start, re-throw exception and abort the start of the connector.
        if (error != null) {
            LOGGER.error("{} task(s) out of {} failed to start.", failedTasks, nTasks);
            throw error;
        }
        else {
            LOGGER.info("All tasks have stated successfully.");
        }
    }

    /**
     * Schedules polling of provided tasks and wait until all polling tasks eventually finish.
     *
     * @param tasks {@link List<EngineSourceTask>} of source tasks which should poll for the records.
     */
    private void runTasksPolling(final List<EngineSourceTask> tasks)
            throws ExecutionException {
        LOGGER.debug("Starting tasks polling.");
        final ExecutorCompletionService<Void> taskCompletionService = new ExecutorCompletionService(taskService);
        final String processorClassName = selectRecordProcessor();
        for (EngineSourceTask task : tasks) {
            final RecordProcessor processor = createRecordProcessor(processorClassName, task);
            processor.initialize(recordService, transformations);
            pollingFutures.add(taskCompletionService.submit(new PollRecords(task, processor, state)));
        }

        for (int i = 0; i < tasks.size(); i++) {
            try {
                taskCompletionService.take().get();
            }
            catch (InterruptedException e) {
                LOGGER.info("Task interrupted while polling.");
            }
            LOGGER.debug("Task #{} out of {} tasks has stopped polling.", i, tasks.size());
        }
    }

    /**
     * Select {@link RecordProcessor} class based on the user configuration.
     *
     * @return Name of the class which should be used for creating {@link RecordProcessor} instances.
     */
    private String selectRecordProcessor() {
        // If the change consumer is provided, it has precedence over the consumer.
        if (handler != null && recordConverter == null) {
            LOGGER.info("Using {} processor", ParallelSmtBatchProcessor.class.getName());
            return ParallelSmtBatchProcessor.class.getName();
        }
        if (handler != null && recordConverter != null) {
            LOGGER.info("Using {} processor", ParallelSmtAndConvertBatchProcessor.class.getName());
            return ParallelSmtAndConvertBatchProcessor.class.getName();
        }

        // Only Consumer is used, records may be processed non-sequentially.
        final RecordProcessingOrder processingOrder = RecordProcessingOrder.parse(this.config.getString(AsyncEngineConfig.RECORD_PROCESSING_ORDER));
        if (processingOrder == RecordProcessingOrder.ORDERED && recordConverter == null) {
            LOGGER.info("Using {} processor", ParallelSmtConsumerProcessor.class.getName());
            return ParallelSmtConsumerProcessor.class.getName();
        }
        if (processingOrder == RecordProcessingOrder.ORDERED && recordConverter != null) {
            LOGGER.info("Using {} processor", ParallelSmtAndConvertConsumerProcessor.class.getName());
            return ParallelSmtAndConvertConsumerProcessor.class.getName();
        }
        if (processingOrder == RecordProcessingOrder.UNORDERED && recordConverter == null) {
            LOGGER.info("Using {} processor", ParallelSmtAsyncConsumerProcessor.class.getName());
            return ParallelSmtAsyncConsumerProcessor.class.getName();
        }
        if (processingOrder == RecordProcessingOrder.UNORDERED && recordConverter != null) {
            LOGGER.info("Using {} processor", ParallelSmtAndConvertAsyncConsumerProcessor.class.getName());
            return ParallelSmtAndConvertAsyncConsumerProcessor.class.getName();
        }

        throw new IllegalStateException("Unable to select RecordProcessor, this should never happen.");
    }

    /**
     * Instantiate {@link RecordProcessor} based on the class name deremined in {@link AsyncEmbeddedEngine#selectRecordProcessor()} method.
     *
     * @return {@link RecordProcessor} instance which will be used for processing the records.
     */
    private RecordProcessor createRecordProcessor(String processorClassName, EngineSourceTask task) {
        if (ParallelSmtBatchProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtBatchProcessor(new SourceRecordCommitter(task), (DebeziumEngine.ChangeConsumer<SourceRecord>) handler);
        }
        if (ParallelSmtAndConvertBatchProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtAndConvertBatchProcessor(new ConvertingRecordCommitter(task), handler, recordConverter);
        }
        if (ParallelSmtConsumerProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtConsumerProcessor(new SourceRecordCommitter(task), (Consumer<SourceRecord>) consumer);
        }
        if (ParallelSmtAndConvertConsumerProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtAndConvertConsumerProcessor(new SourceRecordCommitter(task), consumer, recordConverter);
        }
        if (ParallelSmtAsyncConsumerProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtAsyncConsumerProcessor(new SourceRecordCommitter(task), (Consumer<SourceRecord>) consumer);
        }
        if (ParallelSmtAndConvertAsyncConsumerProcessor.class.getName().equals(processorClassName)) {
            return new ParallelSmtAndConvertAsyncConsumerProcessor(new SourceRecordCommitter(task), consumer, recordConverter);
        }
        throw new IllegalStateException("Unable to create RecordProcessor instance, this should never happen.");
    }

    /**
     * Shuts down the {@link ExecutorService} which processes the change event records.
     * Waits {@code RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS} milliseconds for already submitted records to finish.
     * If the specified timeout is exceeded, the service is shut down immediately.
     */
    private void stopRecordService() {
        LOGGER.debug("Stopping records service.");
        final long shutdownTimeout = config.getLong(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS);
        try {
            recordService.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.info("Timed out while waiting for record service shutdown. Shutting it down immediately.");
            recordService.shutdownNow();
        }
    }

    /**
     * Stops task polling if they haven't stopped yet. Some tasks may be stuck in the polling, we should interrupt such tasks.
     */
    private void stopPollingIfNeeded() {
        for (Future<Void> pollingFuture : pollingFutures) {
            if (!pollingFuture.isDone()) {
                pollingFuture.cancel(true);
            }
        }
    }

    /**
     * Stops all the connector's tasks. There are no checks if the tasks were fully stated or already running, stop is always called.
     * Also tries to stop all the other tasks which may be still running or awaiting execution in the task's thread pool.
     *
     * @param tasks {@link List<EngineSourceTask>} of source tasks which should be stopped.
     */
    private void stopSourceTasks(final List<EngineSourceTask> tasks) {
        try {
            LOGGER.debug("Stopping source connector tasks.");
            final ExecutorCompletionService<Void> taskCompletionService = new ExecutorCompletionService(taskService);
            for (EngineSourceTask task : tasks) {
                final long commitTimeout = Configuration.from(task.context().config()).getLong(EmbeddedEngineConfig.OFFSET_COMMIT_TIMEOUT_MS);
                taskCompletionService.submit(() -> {
                    LOGGER.debug("Committing task's offset.");
                    commitOffsets(task.context().offsetStorageWriter(), task.context().clock(), commitTimeout, task.connectTask());
                    LOGGER.debug("Stopping Connect task.");
                    task.connectTask().stop();
                    return null;
                });
            }

            final long taskStopTimeout = config.getLong(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS);
            LOGGER.debug("Waiting max. for {} ms for individual source tasks to stop.", taskStopTimeout);
            final int nTasks = tasks.size();
            final long startTime = System.nanoTime();
            for (int i = 0; i < nTasks; i++) {
                final Future<Void> taskFuture = taskCompletionService.poll(taskStopTimeout, TimeUnit.MILLISECONDS);
                if (taskFuture != null) {
                    taskFuture.get(0, TimeUnit.MILLISECONDS); // we need to retrieve the results to propagate eventual exceptions
                }
                else {
                    throw new InterruptedException("Time out while waiting for source task to stop.");
                }
                // TODO move back to debug level once we stabilize the testsuite (or add similar log on info level for starting the tasks)
                LOGGER.info("Stopped task #{} out of {} tasks (it took {} ms to stop the task).", i + 1, nTasks, (System.nanoTime() - startTime) / 1_000_000);
                LOGGER.debug("Calling connector callback after task is stopped.");
                // TODO improve Debezium API and provide more info to the callback like id and config
                connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStopped);
            }

            // Some threads can still run start or poll tasks.
            LOGGER.debug("Stopping all remaining tasks if there are any.");
            taskService.shutdown();
        }
        catch (InterruptedException e) {
            LOGGER.warn("Stopping of the tasks was interrupted, shutting down immediately.");
        }
        catch (Exception e) {
            LOGGER.warn("Failure during stopping tasks, stopping them immediately. Failed with ", e);
        }
        finally {
            // Make sure task service is shut down and no other tasks can be run.
            taskService.shutdownNow();
        }
    }

    /**
     * Stops connector's tasks if they are already running and then stops connector itself.
     *
     * @param tasks {@link List<EngineSourceTask>} of source task should be stopped now.
     */
    private void stopConnector(final List<EngineSourceTask> tasks, final State engineState) {
        if (State.shouldStopTasks(engineState)) {
            LOGGER.debug("Tasks were already started, stopping record service and tasks.");
            stopRecordService();
            stopPollingIfNeeded();
            stopSourceTasks(tasks);
        }
        LOGGER.debug("Stopping the connector.");
        connector.connectConnector().stop();
        LOGGER.debug("Calling connector callback after connector stop");
        connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStopped);
    }

    /**
     * Calls provided implementation of {@link DebeziumEngine.CompletionCallback}.
     *
     * @param error Error with which the engine has failed, {@code null} if the engine has finished successfully.
     */
    private void callCompletionHandler(final Throwable error) {
        if (error == null) {
            completionCallback.handle(
                    true, String.format("Connector '%s' completed normally.", config.getString(AsyncEngineConfig.CONNECTOR_CLASS)), null);
        }
        else {
            final Throwable realError = error instanceof ExecutionException ? error.getCause() : error;
            completionCallback.handle(false, error.getMessage(), realError);
        }
    }

    /**
     * Gets the current state of the engine.
     *
     * @return current {@link State} of the {@link AsyncEmbeddedEngine}.
     */
    private State getEngineState() {
        return state.get();
    }

    /**
     * Sets the new state of {@link AsyncEmbeddedEngine}.
     * Initial state is always {@code State.CREATING}.
     * State transition checks current engine state and if it doesn't correspond with expected state an exception is thrown as there is likely a bug in engine
     * implementation or usage.
     *
     * @param expectedState expected current {@link State} of the {@link AsyncEmbeddedEngine}.
     * @param requestedState new {@link State} of the {@link AsyncEmbeddedEngine} to be set.
     * @throws IllegalStateException The exception is thrown when expected engine state doesn't match the actual engine state.
     */
    private void setEngineState(final State expectedState, final State requestedState) {
        if (!state.compareAndSet(expectedState, requestedState)) {
            throw new IllegalStateException(
                    String.format("Cannot change engine state to '%s' as the engine is not in expected state '%s', current engine state is '%s'",
                            requestedState, expectedState, state.get()));
        }
        LOGGER.info("Engine state has changed from '{}' to '{}'", expectedState, requestedState);
    }

    /**
     * Validates provided configuration of the Kafka Connect connector and returns its configuration if it's a valid config.
     *
     * @param connector Kafka Connect {@link SourceConnector}.
     * @param connectorClassName Class name of Kafka Connect {@link SourceConnector}.
     * @return {@link Map<String, String>} with connector configuration.
     */
    private Map<String, String> validateAndGetConnectorConfig(final SourceConnector connector, final String connectorClassName) {
        LOGGER.debug("Validating provided connector configuration.");
        final Map<String, String> connectorConfig = workerConfig.originalsStrings();
        final Config validatedConnectorConfig = connector.validate(connectorConfig);
        final ConfigInfos configInfos = AbstractHerder.generateResult(connectorClassName, Collections.emptyMap(), validatedConnectorConfig.configValues(),
                connector.config().groups());
        if (configInfos.errorCount() > 0) {
            final String errors = configInfos.values().stream()
                    .flatMap(v -> v.configValue().errors().stream())
                    .collect(Collectors.joining(" "));
            throw new DebeziumException("Connector configuration is not valid. " + errors);
        }
        LOGGER.debug("Connector configuration is valid.");
        return connectorConfig;
    }

    /**
     * Determines which offset backing store should be used, instantiate it and starts the offset store.
     *
     * @param connectorConfig {@link Map<String, String>} with the connector configuration.
     * @return {@link OffsetBackingStore} instance used by the engine.
     */
    private OffsetBackingStore createAndStartOffsetStore(final Map<String, String> connectorConfig) throws Exception {
        final String offsetStoreClassName = config.getString(AsyncEngineConfig.OFFSET_STORAGE);

        LOGGER.debug("Creating instance of offset store for {}.", offsetStoreClassName);
        final OffsetBackingStore offsetStore;
        // Kafka 3.5 no longer provides offset stores with non-parametric constructors
        if (offsetStoreClassName.equals(MemoryOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
        }
        else if (offsetStoreClassName.equals(FileOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.fileOffsetBackingStore();
        }
        else if (offsetStoreClassName.equals(KafkaOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.kafkaOffsetBackingStore(connectorConfig);
        }
        else {
            final Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
            offsetStore = offsetStoreClass.getDeclaredConstructor().newInstance();
        }

        try {
            LOGGER.debug("Starting offset store.");
            offsetStore.configure(workerConfig);
            offsetStore.start();
        }
        catch (Throwable t) {
            LOGGER.debug("Failed to start offset store, stopping it now.");
            offsetStore.stop();
            throw t;
        }

        LOGGER.debug("Offset store {} successfully started.", offsetStoreClassName);
        return offsetStore;
    }

    /**
     * Commits the offset to {@link OffsetBackingStore} via {@link OffsetStorageWriter}.
     *
     * @param offsetWriter {@link OffsetStorageWriter} which performs the flushing the offset into {@link OffsetBackingStore}.
     * @param commitTimeout amount of time to wait for offset flush to finish before it's aborted.
     * @param task {@link SourceTask} which performs the offset commit.
     * @return {@code true} if the offset was successfully committed, {@code false} otherwise.
     */
    private static boolean commitOffsets(final OffsetStorageWriter offsetWriter, final io.debezium.util.Clock clock, final long commitTimeout, final SourceTask task)
            throws InterruptedException, TimeoutException {
        final long timeout = clock.currentTimeInMillis() + commitTimeout;
        if (!offsetWriter.beginFlush(commitTimeout, TimeUnit.MICROSECONDS)) {
            LOGGER.trace("No offset to be committed.");
            return false;
        }

        final Future<Void> flush = offsetWriter.doFlush((Throwable error, Void result) -> {
        });
        if (flush == null) {
            LOGGER.warn("Flushing process probably failed, please check previous log for more details.");
            return false;
        }

        try {
            flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
            task.commit();
        }
        catch (InterruptedException e) {
            LOGGER.debug("Flush of the offsets interrupted, canceling the flush.");
            offsetWriter.cancelFlush();
            throw e;
        }
        catch (ExecutionException | TimeoutException e) {
            LOGGER.warn("Flush of the offsets failed, canceling the flush.");
            offsetWriter.cancelFlush();
            return false;
        }
        return true;
    }

    /**
     * Implementation of {@link DebeziumEngine.Builder} which creates {@link AsyncEmbeddedEngine}.
     */
    public static final class AsyncEngineBuilder<R> implements DebeziumEngine.Builder<R> {

        private Properties config;
        private Consumer<R> consumer;
        private DebeziumEngine.ChangeConsumer<?> handler;
        private ClassLoader classLoader;
        private io.debezium.util.Clock clock;
        private DebeziumEngine.CompletionCallback completionCallback;
        private DebeziumEngine.ConnectorCallback connectorCallback;
        private OffsetCommitPolicy offsetCommitPolicy = null;
        private HeaderConverter headerConverter;
        private Function<SourceRecord, R> recordConverter;
        private ConverterBuilder converterBuilder;

        AsyncEngineBuilder() {
            this((KeyValueHeaderChangeEventFormat<?, ?, ?>) null);
        }

        AsyncEngineBuilder(ChangeEventFormat<?> format) {
            this(KeyValueHeaderChangeEventFormat.of(null, format.getValueFormat(), null));
        }

        AsyncEngineBuilder(KeyValueChangeEventFormat<?, ?> format) {
            this(format instanceof KeyValueHeaderChangeEventFormat ? (KeyValueHeaderChangeEventFormat) format
                    : KeyValueHeaderChangeEventFormat.of(format.getKeyFormat(), format.getValueFormat(), null));
        }

        AsyncEngineBuilder(KeyValueHeaderChangeEventFormat<?, ?, ?> format) {
            if (format != null) {
                this.converterBuilder = new ConverterBuilder();
                this.converterBuilder.using(format);
            }
        }

        @Override
        public Builder<R> notifying(final Consumer<R> consumer) {
            this.consumer = consumer;
            if (config.contains(AsyncEngineConfig.RECORD_PROCESSING_WITH_SERIAL_CONSUMER.name())
                    && config.getProperty(AsyncEngineConfig.RECORD_PROCESSING_WITH_SERIAL_CONSUMER.name()).equalsIgnoreCase("true")) {
                if (recordConverter == null) {
                    this.handler = buildDefaultChangeConsumer((Consumer<SourceRecord>) consumer);
                }
                else {
                    this.handler = buildConvertingChangeConsumer(consumer, recordConverter);
                }
            }
            return this;
        }

        @Override
        public Builder<R> notifying(final ChangeConsumer<R> handler) {
            this.handler = handler;
            if (!config.contains(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name()) && !handler.supportsTombstoneEvents()) {
                LOGGER.info("Consumer doesn't support tombstone events, setting '{}' to false.", CommonConnectorConfig.TOMBSTONES_ON_DELETE.name());
                config.put(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name(), "false");
            }
            return this;
        }

        @Override
        public Builder<R> using(final Properties config) {
            this.config = config;
            if (converterBuilder != null) {
                converterBuilder.using(config);
            }
            return this;
        }

        @Override
        public Builder<R> using(final ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        @Override
        public Builder<R> using(final Clock clock) {
            this.clock = clock::millis;
            return this;
        }

        @Override
        public Builder<R> using(final CompletionCallback completionCallback) {
            this.completionCallback = completionCallback;
            return this;
        }

        @Override
        public Builder<R> using(final ConnectorCallback connectorCallback) {
            this.connectorCallback = connectorCallback;
            return this;
        }

        @Override
        public Builder<R> using(final OffsetCommitPolicy policy) {
            this.offsetCommitPolicy = policy;
            return this;
        }

        @Override
        public DebeziumEngine<R> build() {
            if (converterBuilder != null) {
                headerConverter = converterBuilder.headerConverter();
                recordConverter = converterBuilder.toFormat(headerConverter);
            }
            return new AsyncEmbeddedEngine(config, consumer, handler, classLoader, clock, completionCallback, connectorCallback, offsetCommitPolicy, headerConverter,
                    recordConverter);
        }
    }

    /**
     * Build the default {@link DebeziumEngine.ChangeConsumer} from provided custom {@link Consumer}.
     *
     * @param consumer {@link Consumer} provided by the user.
     * @return {@link DebeziumEngine.ChangeConsumer} which use user-provided {@link Consumer} for processing the Debezium records.
     */
    private static ChangeConsumer<SourceRecord> buildDefaultChangeConsumer(Consumer<SourceRecord> consumer) {
        return new DebeziumEngine.ChangeConsumer<>() {

            /**
             * The default implementation of {@link DebeziumEngine.ChangeConsumer}.
             * On every record, it calls the consumer, and then only marks the record
             * as processed when accept returns. Additionally, it handles StopEngineException
             * and ensures that we always try and mark a batch as finished, even with exceptions.
             *
             * @param records the records to be processed
             * @param committer the committer that indicates to the system that we are finished
             *
             * @throws Exception
             */
            @Override
            public void handleBatch(final List<SourceRecord> records, final DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
                for (SourceRecord record : records) {
                    try {
                        consumer.accept(record);
                        committer.markProcessed(record);
                    }
                    catch (StopEngineException ex) {
                        // Ensure that we mark the record as finished in this case.
                        committer.markProcessed(record);
                        throw ex;
                    }
                }
                committer.markBatchFinished();
            }
        };
    }

    /**
     * Build the {@link DebeziumEngine.ChangeConsumer} from provided custom {@link Consumer} which convert records to requested format before passing them
     * to the custom {@link Consumer}.
     *
     * @param consumer {@link Consumer} provided by the user.
     * @return {@link DebeziumEngine.ChangeConsumer} which use user-provided {@link Consumer} for processing the Debezium records.
     */
    private static ChangeConsumer buildConvertingChangeConsumer(Consumer consumer, Function<SourceRecord, ?> recordConverter) {
        return new DebeziumEngine.ChangeConsumer<SourceRecord>() {

            @Override
            public void handleBatch(final List<SourceRecord> records, final DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
                for (SourceRecord record : records) {
                    try {
                        consumer.accept(recordConverter.apply(record));
                        committer.markProcessed(record);
                    }
                    catch (StopEngineException ex) {
                        // Ensure that we mark the record as finished in this case.
                        committer.markProcessed(record);
                        throw ex;
                    }
                }
                committer.markBatchFinished();
            }
        };
    }

    /**
     * Determines the size of the thread pool which will be used for processing records. The value can be either number (provided as a {@code String} value) or
     * a predefined placeholder from {@link ProcessingCores} enumeration. If the number of threads is provided as a number, it will be eventually limited to
     * {@code AsyncEngineConfig.RECORD_PROCESSING_THREADS_CAP} to avoid possible overhead with too many context switches on a beefy machines with many cores, but
     * running many other tasks.
     *
     * @param recordProcessingThreads Requested number of processing threads as a {@code String}. It can be a number or predefined placeholder.
     * @return Either requested number of threads or minimum of {@code AsyncEngineConfig.RECORD_PROCESSING_THREADS_CAP} and {@code AsyncEngineConfig.AVAILABLE_CORES}.
     */
    private int computeRecordThreads(final String recordProcessingThreads) {
        // First check if it's some our placeholder constant.
        final ProcessingCores pc = ProcessingCores.parse(recordProcessingThreads);
        if (pc != null) {
            return pc.getCores();
        }

        // If it's not a placeholder, assume it's a number and eventually throw an exception is it's not.
        final int cores = Integer.valueOf(recordProcessingThreads);
        if (cores <= 0) {
            throw new IllegalArgumentException("Number of cores cannot be negative or zero!");
        }

        // Now we apply processor cap. As we provide all available cores as the default value, we eventually reduce the value now.
        return Math.min(cores, AsyncEngineConfig.RECORD_PROCESSING_THREADS_CAP);
    }

    /**
     * Enum with possible placeholders for number of cores to be used record processing.
     * Currently only {@code AVAILABLE_CORES} for using all available cores is supported.
     */
    private enum ProcessingCores {
        // Use all available cores of the machine.
        AVAILABLE_CORES("AVAILABLE_CORES", AsyncEngineConfig.AVAILABLE_CORES);

        private final String coresPlaceholder;
        private final int cores;

        ProcessingCores(final String coresPlaceholder, final int cores) {
            this.coresPlaceholder = coresPlaceholder;
            this.cores = cores;
        }

        public int getCores() {
            return cores;
        }

        public static ProcessingCores parse(final String value) {
            if (value == null) {
                return null;
            }
            final String trimmedValue = value.trim();
            for (ProcessingCores processingCores : ProcessingCores.values()) {
                if (processingCores.coresPlaceholder.equalsIgnoreCase(trimmedValue)) {
                    return processingCores;
                }
            }
            return null;
        }
    }

    /**
     * Determines how the records will be processed.
     * Currently only sequential processing ("ORDERED") and non-sequential processing ("UNORDERED") modes are supported.
     */
    private enum RecordProcessingOrder {
        // All records will be processed in the same order in which were obtained from the database.
        ORDERED("ORDERED"),
        // Records will be processed in completely arbitrary order.
        UNORDERED("UNORDERED");
        // TODO
        // Records with the same key will be processed in the same order in which they were obtained from the database, but records with different keys may be processed
        // out of order.
        // ORDERED_PER_KEY

        private final String orderingPlaceholder;

        RecordProcessingOrder(final String orderingPlaceholder) {
            this.orderingPlaceholder = orderingPlaceholder;
        }

        public static RecordProcessingOrder parse(String value) {
            if (value == null) {
                return null;
            }
            final String trimmedValue = value.trim();
            for (RecordProcessingOrder processingOrder : RecordProcessingOrder.values()) {
                if (processingOrder.orderingPlaceholder.equalsIgnoreCase(trimmedValue)) {
                    return processingOrder;
                }
            }
            return null;
        }
    }

    /**
     * Possible engine states.
     * Engine state must be changed only via {@link AsyncEmbeddedEngine#setEngineState(State, State)} method.
     */
    private enum State {
        // Order of the possible states is important, enum ordinal is used for state comparison.
        CREATING, // the engine is being started, which mostly means engine object is being created or was already created, but run() method wasn't called yet
        INITIALIZING, // initializing the connector
        CREATING_TASKS, // creating connector tasks
        STARTING_TASKS, // starting connector tasks
        POLLING_TASKS, // running tasks polling, this is the main phase when the data are produced
        STOPPING, // the engine is being stopped
        STOPPED; // engine has been stopped, final state, cannot move any further from this state and any call on engine in this state should fail

        /**
         * Given the engine state, determines if the connector tasks were already started and should be stopped.
         *
         * @param state Engine {@link State} when the shutdown was called.
         * @return {@code true} if connector tasks were already started, {@code false otherwise}.
         */
        public static boolean shouldStopTasks(State state) {
            return State.STARTING_TASKS.compareTo(state) <= 0;
        }

        /**
         * Given engine state, determines if engine can be stopped when it's in this state.
         *
         * @param state Engine {@link State} from which engine shutdown is intended to be called.
         * @return {@code true} if engine can be stopped, {@code false} otherwise.
         */
        public static boolean canBeStopped(State state) {
            return State.STOPPING.compareTo(state) > 0;
        }
    }

    /**
     * Default completion callback which just logs the error. If connector finishes successfully it does nothing.
     */
    private static class DefaultCompletionCallback implements DebeziumEngine.CompletionCallback {
        @Override
        public void handle(final boolean success, final String message, final Throwable error) {
            if (!success) {
                LOGGER.error(message, error);
            }
        }
    }

    /**
     * {@link Callable} which in the loop polls the connector for the records.
     * If there are any records, they are passed to the provided processor.
     * The {@link Callable} is {@link RetryingCallable} - if the {@link org.apache.kafka.connect.errors.RetriableException}
     * is thrown, the {@link Callable} is executed again according to configured {@link DelayStrategy} and number of retries.
     */
    private static class PollRecords extends RetryingCallable<Void> {
        final EngineSourceTask task;
        final RecordProcessor processor;
        final AtomicReference<State> engineState;

        PollRecords(final EngineSourceTask task, final RecordProcessor processor, final AtomicReference<State> engineState) {
            super(Configuration.from(task.context().config()).getInteger(EmbeddedEngineConfig.ERRORS_MAX_RETRIES));
            this.task = task;
            this.processor = processor;
            this.engineState = engineState;
        }

        @Override
        public Void doCall() throws Exception {
            while (engineState.get() == State.POLLING_TASKS) {
                LOGGER.trace("Thread {} running task {} starts polling for records.", Thread.currentThread().getName(), task.connectTask());
                final List<SourceRecord> changeRecords = task.connectTask().poll(); // blocks until there are values ...
                LOGGER.trace("Thread {} polled {} records.", Thread.currentThread().getName(), changeRecords == null ? "no" : changeRecords.size());
                if (changeRecords != null && !changeRecords.isEmpty()) {
                    processor.processRecords(changeRecords);
                }
                else {
                    LOGGER.trace("No records.");
                }
            }
            return null;
        }

        @Override
        public DelayStrategy delayStrategy() {
            final Configuration config = Configuration.from(task.context().config());
            return DelayStrategy.exponential(Duration.ofMillis(config.getInteger(EmbeddedEngineConfig.ERRORS_RETRY_DELAY_INITIAL_MS)),
                    Duration.ofMillis(config.getInteger(EmbeddedEngineConfig.ERRORS_RETRY_DELAY_MAX_MS)));
        }
    }

    /**
     * The default implementation of {@link DebeziumEngine.RecordCommitter}.
     * The implementation is not thread safe and the caller has to ensure it's used in thread safe manner.
     */
    private static class SourceRecordCommitter implements DebeziumEngine.RecordCommitter<SourceRecord> {

        final SourceTask task;
        final OffsetStorageWriter offsetWriter;
        final OffsetCommitPolicy offsetCommitPolicy;
        final io.debezium.util.Clock clock;
        final long commitTimeout;

        private long recordsSinceLastCommit = 0;
        private long timeOfLastCommitMillis = 0;

        SourceRecordCommitter(final EngineSourceTask task) {
            this.task = task.connectTask();
            this.offsetWriter = task.context().offsetStorageWriter();
            this.offsetCommitPolicy = task.context().offsetCommitPolicy();
            this.clock = task.context().clock();
            this.commitTimeout = Configuration.from(task.context().config()).getLong(EmbeddedEngineConfig.OFFSET_COMMIT_TIMEOUT_MS);
        }

        @Override
        public void markProcessed(SourceRecord record) throws InterruptedException {
            task.commitRecord(record);
            recordsSinceLastCommit += 1;
            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
        }

        @Override
        public void markBatchFinished() throws InterruptedException {
            final Duration durationSinceLastCommit = Duration.ofMillis(clock.currentTimeInMillis() - timeOfLastCommitMillis);
            if (offsetCommitPolicy.performCommit(recordsSinceLastCommit, durationSinceLastCommit)) {
                try {
                    if (commitOffsets(offsetWriter, clock, commitTimeout, task)) {
                        recordsSinceLastCommit = 0;
                        timeOfLastCommitMillis = clock.currentTimeInMillis();
                    }
                }
                catch (TimeoutException e) {
                    throw new DebeziumException("Timed out while waiting for committing task offset", e);
                }
            }
        }

        @Override
        public void markProcessed(SourceRecord record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
            DebeziumEngineCommon.SourceRecordOffsets offsets = (DebeziumEngineCommon.SourceRecordOffsets) sourceOffsets;
            SourceRecord recordWithUpdatedOffsets = new SourceRecord(record.sourcePartition(), offsets.getOffsets(), record.topic(),
                    record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(),
                    record.timestamp(), record.headers());
            markProcessed(recordWithUpdatedOffsets);
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return new DebeziumEngineCommon.SourceRecordOffsets();
        }
    }

    /**
     * Implementation of {@link DebeziumEngine.RecordCommitter} which convert records to {@link SourceRecord}s and pass them to {@link SourceRecordCommitter}.
     * The implementation is not thread safe and the caller has to ensure it's used in thread safe manner.
     */
    private class ConvertingRecordCommitter implements DebeziumEngine.RecordCommitter<R> {

        private final SourceRecordCommitter delegate;

        ConvertingRecordCommitter(final EngineSourceTask task) {
            this.delegate = new SourceRecordCommitter(task);
        }

        @Override
        public void markProcessed(R record) throws InterruptedException {
            delegate.markProcessed(sourceConverter.apply(record));
        }

        @Override
        public void markBatchFinished() throws InterruptedException {
            delegate.markBatchFinished();
        }

        @Override
        public void markProcessed(R record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
            delegate.markProcessed(sourceConverter.apply(record), sourceOffsets);
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return delegate.buildOffsets();
        }
    }
}

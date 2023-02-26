/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;

public class TaskWorker {

    static final int DEFAULT_ERROR_MAX_RETRIES = -1;

    static final Field ERRORS_MAX_RETRIES = Field.create("errors.max.retries")
            .withDisplayName("The maximum number of retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_ERROR_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription("The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, > 0 = num of retries).");

    static final Field ERRORS_RETRY_DELAY_INITIAL_MS = Field.create("errors.retry.delay.initial.ms")
            .withDisplayName("Initial delay for retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(300)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Initial delay (in ms) for retries when encountering connection errors."
                    + " This value will be doubled upon every retry but won't exceed 'errors.retry.delay.max.ms'.");

    static final Field ERRORS_RETRY_DELAY_MAX_MS = Field.create("errors.retry.delay.max.ms")
            .withDisplayName("Max delay between retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(10000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Max delay (in ms) between retries when encountering connection errors.");

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskWorker.class);

    private final int taskId;
    private final Class<? extends Task> taskClass;
    private final EmbeddedEngineState embeddedEngineState;
    private final DebeziumEngine.ChangeConsumer<SourceRecord> handler;
    private final Transformations transformations;
    private SourceTask task;
    private TaskOffsetManager taskOffsetManager;
    private final Clock clock;
    private final Optional<DebeziumEngine.ConnectorCallback> connectorCallback;
    private final EmbeddedEngine.CompletionResult completionResult;
    private Map<String, String> taskConfig;

    private int maxRetries;
    private DelayStrategy delayStrategy;

    public TaskWorker(
                      int taskId,
                      Class<? extends Task> taskClass, EmbeddedEngineState embeddedEngineState,
                      DebeziumEngine.ChangeConsumer<SourceRecord> handler,
                      Transformations transformations,
                      Clock clock,
                      DebeziumEngine.ConnectorCallback connectorCallback,
                      EmbeddedEngine.CompletionResult completionResult) {
        this.taskId = taskId;
        this.taskClass = taskClass;
        this.embeddedEngineState = embeddedEngineState;
        this.handler = handler;
        this.transformations = transformations;
        this.clock = clock;
        this.connectorCallback = Optional.ofNullable(connectorCallback);
        this.completionResult = completionResult;
    }

    public void configure(Configuration config) {

        task = null;
        try {
            task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
        }
        catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException t) {
            fail("Unable to instantiate connector's task class '" + taskClass.getName() + "'", t);
            return;
        }

        this.taskConfig = config.asMap();

        taskOffsetManager = new DefaultTaskOffsetManager(
                this.clock, task, embeddedEngineState);
        taskOffsetManager.configure(config);

        this.maxRetries = config.getInteger(ERRORS_MAX_RETRIES);
        this.delayStrategy = DelayStrategy.exponential(Duration.ofMillis(config.getInteger(ERRORS_RETRY_DELAY_INITIAL_MS)),
                Duration.ofMillis(config.getInteger(ERRORS_RETRY_DELAY_MAX_MS)));
    }

    public SourceTask task() {
        return task;
    }

    public void run() {
        try {
            SourceTaskContext taskContext = new SourceTaskContext() {
                @Override
                public OffsetStorageReader offsetStorageReader() {
                    return taskOffsetManager.offsetStorageReader();
                }

                // Purposely not marking this method with @Override as it was introduced in Kafka 2.x
                // and otherwise would break builds based on Kafka 1.x
                public Map<String, String> configs() {
                    return taskConfig;
                }
            };
            task.initialize(taskContext);
            task.start(taskConfig);
            connectorCallback.ifPresent(c -> c.taskStarted(taskId));
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
            Configuration config = Configuration.from(taskConfig).withMaskedPasswords();
            String msg = "Unable to initialize and start connector's task class '" + task.getClass().getName() + "' with config: "
                    + config;
            fail(msg, t);
            return;
        }

        Throwable handlerError = null, retryError = null;

        try {
            EmbeddedEngine.RecordCommitter committer = EmbeddedEngine.buildRecordCommitter(taskOffsetManager);
            while (embeddedEngineState.isRunning()) {
                List<SourceRecord> changeRecords = null;
                try {
                    LOGGER.debug("Embedded engine is polling task for records on thread {}", Thread.currentThread());
                    changeRecords = task.poll(); // blocks until there are values ...
                    LOGGER.debug("Embedded engine returned from polling task for records");
                }
                catch (InterruptedException e) {
                    // Interrupted while polling ...
                    LOGGER.debug("Embedded engine interrupted on thread {} while polling the task for records", Thread.currentThread());
                    if (this.embeddedEngineState.isRunning()) {
                        // the engine is still marked as running -> we were not interrupted
                        // due the stop() call -> probably someone else called the interrupt on us ->
                        // -> we should raise the interrupt flag
                        Thread.currentThread().interrupt();
                    }
                    break;
                }
                catch (RetriableException e) {
                    LOGGER.info("Retriable exception thrown, connector will be restarted; errors.max.retries={}", maxRetries, e);
                    if (maxRetries < DEFAULT_ERROR_MAX_RETRIES) {
                        retryError = e;
                        throw e;
                    }
                    else if (maxRetries != 0) {
                        int totalRetries = 0;
                        boolean startedSuccessfully = false;
                        while (!startedSuccessfully) {
                            try {
                                totalRetries++;
                                LOGGER.info("Starting connector, attempt {}", totalRetries);
                                task.stop();
                                task.start(taskConfig);
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
                                .filter(Objects::nonNull)
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
                succeed("Task '" + task.getClass().getName() + taskId + "' completed normally.");
            }
            try {
                // First stop the task ...
                LOGGER.info("Stopping the task and engine");
                task.stop();
                connectorCallback.ifPresent(c -> c.taskStopped(taskId));
                // Always commit offsets that were captured from the source records we actually processed ...
                try {
                    taskOffsetManager.commitOffsets();
                }
                finally {
                    taskOffsetManager.stop();
                }
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
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.EmbeddedEngine.CompletionCallback;
import io.debezium.embedded.EmbeddedEngine.ConnectorCallback;
import io.debezium.embedded.EmbeddedEngine.EmbeddedConfig;
import io.debezium.engine.DebeziumEngine;
import io.debezium.function.BooleanConsumer;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.TestLogger;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.pipeline.txmetadata.TransactionStatus;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;

/**
 * An abstract base class for unit testing {@link SourceConnector} implementations using the Debezium {@link EmbeddedEngine}
 * with local file storage.
 * <p>
 * To use this abstract class, simply create a test class that extends it, and add one or more test methods that
 * {@link #start(Class, Configuration) starts the connector} using your connector's custom configuration.
 * Then, your test methods can call {@link #consumeRecords(int, Consumer)} to consume the specified number
 * of records (the supplied function gives you a chance to do something with the record).
 *
 * @author Randall Hauch
 */
public abstract class AbstractConnectorTest implements Testing {

    @Rule
    public TestRule skipTestRule = new SkipTestRule();

    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    private ExecutorService executor;
    protected EmbeddedEngine engine;
    protected BlockingQueue<SourceRecord> consumedLines;
    protected long pollTimeoutInMs = TimeUnit.SECONDS.toMillis(10);
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private CountDownLatch latch;
    private JsonConverter keyJsonConverter = new JsonConverter();
    private JsonConverter valueJsonConverter = new JsonConverter();
    private JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
    private JsonDeserializer valueJsonDeserializer = new JsonDeserializer();
    private boolean skipAvroValidation = false;

    @Rule
    public TestRule logTestName = new TestLogger(logger);

    @Before
    public final void initializeConnectorTestFramework() {
        LoggingContext.forConnector(getClass().getSimpleName(), "", "test");
        keyJsonConverter = new JsonConverter();
        valueJsonConverter = new JsonConverter();
        keyJsonDeserializer = new JsonDeserializer();
        valueJsonDeserializer = new JsonDeserializer();
        Configuration converterConfig = Configuration.create().build();
        Configuration deserializerConfig = Configuration.create().build();
        keyJsonConverter.configure(converterConfig.asMap(), true);
        valueJsonConverter.configure(converterConfig.asMap(), false);
        keyJsonDeserializer.configure(deserializerConfig.asMap(), true);
        valueJsonDeserializer.configure(deserializerConfig.asMap(), false);

        resetBeforeEachTest();
        consumedLines = new ArrayBlockingQueue<>(getMaximumEnqueuedRecordCount());
        Testing.Files.delete(OFFSET_STORE_PATH);
        OFFSET_STORE_PATH.getParent().toFile().mkdirs();
    }

    /**
     * Stop the connector and block until the connector has completely stopped.
     */
    @After
    public final void stopConnector() {
        stopConnector(null);
    }

    /**
     * Stop the connector, and return whether the connector was successfully stopped.
     *
     * @param callback the function that should be called with whether the connector was successfully stopped; may be null
     */
    public void stopConnector(BooleanConsumer callback) {
        try {
            logger.info("Stopping the connector");
            // Try to stop the connector ...
            if (engine != null && engine.isRunning()) {
                logger.info("Stopping the engine");
                engine.stop();
                try {
                    // Oracle connector needs longer time to complete shutdown
                    engine.await(60, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    logger.warn("Engine has not stopped on time");
                    Thread.currentThread().interrupt();
                }
            }
            if (executor != null) {
                logger.info("Interrupting the engine");
                List<Runnable> neverRunTasks = executor.shutdownNow();
                assertThat(neverRunTasks).isEmpty();
                try {
                    while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        // wait for completion ...
                    }
                }
                catch (InterruptedException e) {
                    logger.warn("Executor has not stopped on time");
                    Thread.currentThread().interrupt();
                }
            }
            if (engine != null && engine.isRunning()) {
                logger.info("Waiting for engine to stop");
                try {
                    while (!engine.await(60, TimeUnit.SECONDS)) {
                        // Wait for connector to stop completely ...
                    }
                }
                catch (InterruptedException e) {
                    logger.warn("Connector has not stopped on time");
                    Thread.currentThread().interrupt();
                }
            }
            if (callback != null) {
                callback.accept(engine != null && engine.isRunning());
            }
        }
        finally {
            engine = null;
            executor = null;
        }
    }

    /**
     * Get the maximum number of messages that can be obtained from the connector and held in-memory before they are
     * consumed by test methods using {@link #consumeRecord()}, {@link #consumeRecords(int)}, or
     * {@link #consumeRecords(int, Consumer)}.
     *
     * <p>
     * By default this method return {@code 100}.
     *
     * @return the maximum number of records that can be enqueued
     */
    protected int getMaximumEnqueuedRecordCount() {
        return 100;
    }

    /**
     * Create a {@link CompletionCallback} that logs when the engine fails to start the connector or when the connector
     * stops running after completing successfully or due to an error
     *
     * @return the logging {@link CompletionCallback}
     */
    protected CompletionCallback loggingCompletion() {
        return (success, msg, error) -> {
            if (success) {
                logger.info(msg);
            }
            else {
                logger.error(msg, error);
            }
        };
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig) {
        start(connectorClass, connectorConfig, loggingCompletion(), null);
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged. Records arriving after connector stop must not be ignored.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     */
    protected void startAndConsumeTillEnd(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig) {
        start(connectorClass, connectorConfig, loggingCompletion(), null, x -> {
        }, false);
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged. The connector will stop immediately when the supplied predicate returns true.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         Predicate<SourceRecord> isStopRecord) {
        start(connectorClass, connectorConfig, loggingCompletion(), isStopRecord);
    }

    /**
     * Start the connector using the supplied connector configuration, where upon completion the status of the connector is
     * logged. Records arriving after connector stop must not be ignored.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     */
    protected void startAndConsumeTillEnd(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                                          Predicate<SourceRecord> isStopRecord) {
        start(connectorClass, connectorConfig, loggingCompletion(), isStopRecord, x -> {
        }, false);
    }

    /**
     * Start the connector using the supplied connector configuration.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         DebeziumEngine.CompletionCallback callback) {
        start(connectorClass, connectorConfig, callback, null);
    }

    /**
     * Start the connector using the supplied connector configuration.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         DebeziumEngine.CompletionCallback callback, Predicate<SourceRecord> isStopRecord) {
        start(connectorClass, connectorConfig, callback, isStopRecord, x -> {
        }, true);
    }

    /**
     * Start the connector using the supplied connector configuration.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param changeConsumer {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} invoked when a record arrives and is stored in the queue
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         DebeziumEngine.ChangeConsumer changeConsumer) {
        start(connectorClass, connectorConfig, loggingCompletion(), null, x -> {
        }, true, changeConsumer);
    }

    /**
     * Start the connector using the supplied connector configuration.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     * @param recordArrivedListener function invoked when a record arrives and is stored in the queue
     * @param ignoreRecordsAfterStop {@code true} if records arriving after stop should be ignored
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         DebeziumEngine.CompletionCallback callback, Predicate<SourceRecord> isStopRecord,
                         Consumer<SourceRecord> recordArrivedListener, boolean ignoreRecordsAfterStop) {
        start(connectorClass, connectorConfig, callback, isStopRecord, recordArrivedListener, ignoreRecordsAfterStop, null);
    }

    /**
     * Start the connector using the supplied connector configuration.
     *
     * @param connectorClass the connector class; may not be null
     * @param connectorConfig the configuration for the connector; may not be null
     * @param isStopRecord the function that will be called to determine if the connector should be stopped before processing
     *            this record; may be null if not needed
     * @param callback the function that will be called when the engine fails to start the connector or when the connector
     *            stops running after completing successfully or due to an error; may be null
     * @param recordArrivedListener function invoked when a record arrives and is stored in the queue
     * @param ignoreRecordsAfterStop {@code true} if records arriving after stop should be ignored
     * @param changeConsumer {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} invoked when a record arrives and is stored in the queue
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                         DebeziumEngine.CompletionCallback callback, Predicate<SourceRecord> isStopRecord,
                         Consumer<SourceRecord> recordArrivedListener, boolean ignoreRecordsAfterStop, DebeziumEngine.ChangeConsumer changeConsumer) {
        Configuration config = Configuration.copy(connectorConfig)
                .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                .with(EmbeddedEngine.CONNECTOR_CLASS, connectorClass.getName())
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .build();
        latch = new CountDownLatch(1);
        CompletionCallback wrapperCallback = (success, msg, error) -> {
            try {
                if (callback != null) {
                    callback.handle(success, msg, error);
                }
            }
            finally {
                if (!success) {
                    // we only unblock if there was an error; in all other cases we're unblocking when a task has been started
                    latch.countDown();
                }
            }
            Testing.debug("Stopped connector");
        };

        ConnectorCallback connectorCallback = new ConnectorCallback() {
            @Override
            public void taskStarted() {
                // if this is called, it means a task has been started successfully so we can continue
                latch.countDown();
            }
        };

        // Create the connector ...
        EmbeddedEngine.Builder builder = EmbeddedEngine.create();
        builder.using(config)
                .notifying(getConsumer(isStopRecord, recordArrivedListener, ignoreRecordsAfterStop))
                .using(this.getClass().getClassLoader())
                .using(wrapperCallback)
                .using(connectorCallback);
        if (changeConsumer != null) {
            builder.notifying(changeConsumer);
        }
        engine = builder.build();

        // Submit the connector for asynchronous execution ...
        assertThat(executor).isNull();
        executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        try {
            if (!latch.await(5, TimeUnit.MINUTES)) {
                // maybe it takes more time to start up, so just log a warning and continue
                logger.warn("The connector did not finish starting its task(s) or complete in the expected amount of time");
            }

            // This allows existing tests to work without modification since they typically assume the
            // BaseSourceTask#start(Configuration) method has been execute as part of the Task's start method.
            waitForNotInitialState();
        }
        catch (InterruptedException e) {
            if (Thread.interrupted()) {
                fail("Interrupted while waiting for engine startup");
            }
        }
    }

    /**
     * Wait until the Task state it not {@link BaseSourceTask.State#INITIAL}.
     * This indicates that the task has been polled and the internal tasks startIfNecessary method has been called.
     * <p/>
     * This methos will return immediately if the task is not an instance of  {@link BaseSourceTask}.
     */
    protected void waitForNotInitialState() {
        engine.runWithTask(sourceTask -> {
            if (sourceTask instanceof BaseSourceTask) {
                BaseSourceTask<?, ?> baseSourceTask = (BaseSourceTask<?, ?>) sourceTask;
                Awaitility.await()
                        .alias("Task has attempted to initialize coordinator")
                        .pollInterval(100, TimeUnit.MILLISECONDS)
                        .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                        .until(() -> baseSourceTask.getTaskState() != BaseSourceTask.State.INITIAL);

            }
        });
    }

    protected Consumer<SourceRecord> getConsumer(Predicate<SourceRecord> isStopRecord, Consumer<SourceRecord> recordArrivedListener, boolean ignoreRecordsAfterStop) {
        return (record) -> {
            if (isStopRecord != null && isStopRecord.test(record)) {
                logger.error("Stopping connector after record as requested");
                throw new ConnectException("Stopping connector after record as requested");
            }
            // Test stopped the connector, remaining records are ignored
            if (ignoreRecordsAfterStop && (!engine.isRunning() || Thread.currentThread().isInterrupted())) {
                return;
            }
            while (!consumedLines.offer(record)) {
                if (ignoreRecordsAfterStop && (!engine.isRunning() || Thread.currentThread().isInterrupted())) {
                    return;
                }
            }
            recordArrivedListener.accept(record);
        };
    }

    /**
     * Set the maximum amount of time that the {@link #consumeRecord()}, {@link #consumeRecords(int)}, and
     * {@link #consumeRecords(int, Consumer)} methods block while waiting for each record before returning <code>null</code>.
     *
     * @param timeout the timeout; must be positive
     * @param unit the time unit; may not be null
     */
    protected void setConsumeTimeout(long timeout, TimeUnit unit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("The timeout may not be negative");
        }
        pollTimeoutInMs = unit.toMillis(timeout);
    }

    /**
     * Consume a single record from the connector.
     *
     * @return the next record that was returned from the connector, or null if no such record has been produced by the connector
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecord consumeRecord() throws InterruptedException {
        return consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Try to consume the specified number of records from the connector, and return the actual number of records that were
     * consumed. Use this method when your test does not care what the records might contain.
     *
     * @param numberOfRecords the number of records that should be consumed
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecords(int numberOfRecords) throws InterruptedException {
        return consumeRecords(numberOfRecords, null);
    }

    /**
     * Try to consume the specified number of records from the connector, calling the given function for each, and return the
     * actual number of records that were consumed.
     * For slower connectors it is possible to receive no records from the connector multiple times in a row
     * till the waiting is terminated.
     *
     * @param numberOfRecords the number of records that should be consumed
     * @param breakAfterNulls the number of allowed runs when no records are received
     * @param recordConsumer the function that should be called with each consumed record
     * @param assertRecords true if records serialization should be verified
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecords(int numberOfRecords, int breakAfterNulls, Consumer<SourceRecord> recordConsumer, boolean assertRecords) throws InterruptedException {
        return consumeRecordsUntil(
                (recordsConsumed, record) -> recordsConsumed >= numberOfRecords,
                (recordsConsumed, record) -> "Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                        + (numberOfRecords - recordsConsumed) + " more)",
                breakAfterNulls,
                recordConsumer, assertRecords);
    }

    /**
     * Try to consume the records from the connector, until a condition is satisfied.
     * For slower connectors it is possible to receive no records from the connector multiple times in a row
     * till the waiting is terminated.
     *
     * @param condition the condition that decides that consuming has finished
     * @param logMessage diagnostic message printed
     * @param breakAfterNulls the number of allowed runs when no records are received
     * @param recordConsumer the function that should be called with each consumed record
     * @param assertRecords true if records serialization should be verified
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecordsUntil(BiPredicate<Integer, SourceRecord> condition,
                                      BiFunction<Integer, SourceRecord, String> logMessage, int breakAfterNulls,
                                      Consumer<SourceRecord> recordConsumer, boolean assertRecords)
            throws InterruptedException {
        int recordsConsumed = 0;
        int nullReturn = 0;
        boolean isLastRecord = false;
        while (!isLastRecord) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
            if (record != null) {
                nullReturn = 0;
                ++recordsConsumed;
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
                if (Testing.Debug.isEnabled()) {
                    Testing.debug(logMessage.apply(recordsConsumed, record));
                    debug(record);
                }
                else if (Testing.Print.isEnabled()) {
                    Testing.print(logMessage.apply(recordsConsumed, record));
                    print(record);
                }
                if (assertRecords) {
                    VerifyRecord.isValid(record, skipAvroValidation);
                }
                isLastRecord = condition.test(recordsConsumed, record);
            }
            else {
                if (++nullReturn >= breakAfterNulls) {
                    return recordsConsumed;
                }
            }
        }
        return recordsConsumed;
    }

    /**
     * Try to consume the specified number of records from the connector, calling the given function for each, and return the
     * actual number of records that were consumed.
     * For slower connectors it is possible to receive no records from the connector at most 3 times in a row
     * till the waiting is terminated.
     *
     * @param numberOfRecords the number of records that should be consumed
     * @param recordConsumer the function that should be called with each consumed record
     * @return the actual number of records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeRecords(int numberOfRecords, Consumer<SourceRecord> recordConsumer) throws InterruptedException {
        int breakAfterNulls = waitTimeForRecordsAfterNulls();
        return consumeRecords(numberOfRecords, breakAfterNulls, recordConsumer, true);
    }

    /**
     * Try to consume and capture exactly the specified number of records from the connector.
     *
     * @param numRecords the number of records that should be consumed
     * @param breakAfterNulls how many times to wait when no records arrive from the connector
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords, int breakAfterNulls) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        consumeRecords(numRecords, breakAfterNulls, records::add, true);
        return records;
    }

    /**
     * Try to consume and capture exactly the specified number of records from the connector.
     *
     * @param numRecords the number of records that should be consumed
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        consumeRecords(numRecords, records::add);
        return records;
    }

    /**
     * Try to consume and capture exactly the specified number of records from the connector.
     * The initial records are skipped until the condition is satisfied.
     * This is most useful in corner cases when there can be a duplicate records between snapshot
     * and streaming switch.
     *
     * @param numRecords the number of records that should be consumed
     * @param tripCondition condition to satisfy to stop skipping records
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsButSkipUntil(int recordsToRead, BiPredicate<Struct, Struct> tripCondition) throws InterruptedException {
        final var records = new SourceRecords();
        final var skipRecords = new AtomicBoolean(true);
        consumeRecords(recordsToRead, record -> {
            if (skipRecords.get()) {
                if (tripCondition.test((Struct) record.key(), (Struct) record.value())) {
                    skipRecords.set(false);
                }
                else {
                    Testing.print("Skipped record");
                    print(record);
                    Testing.debug("Skipped record");
                    debug(record);
                }
            }
            if (!skipRecords.get()) {
                records.add(record);
            }
        });
        recordsToRead -= records.allRecordsInOrder().size();
        if (recordsToRead > 0) {
            consumeRecords(recordsToRead, records::add);
        }
        return records;
    }

    /**
     * Try to consume and capture records untel a codition is satisfied.
     *
     * @param condition contition that must be satisifed to terminate reading
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsByTopicUntil(BiPredicate<Integer, SourceRecord> condition) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        consumeRecordsUntil(
                condition,
                (recordsConsumed, record) -> "Consumed " + (condition.test(recordsConsumed, record) ? "last " : "") + "record " + recordsConsumed,
                waitTimeForRecordsAfterNulls(),
                records::add,
                true);
        return records;
    }

    /**
     * Try to consume and capture exactly the specified number of records from the connector.
     *
     * @param numRecords the number of records that should be consumed
     * @return the collector into which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords, boolean assertRecords) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        int breakAfterNulls = waitTimeForRecordsAfterNulls();
        consumeRecords(numRecords, breakAfterNulls, records::add, assertRecords);
        return records;
    }

    /**
     * Try to consume and capture exactly the specified number of Dml records from the connector.
     *
     * While transaction metadata topic records are captured by this method, the {@code numDmlRecords} should not
     * include the expected number of records emitted to the transaction topic.
     *
     * @param numDmlRecords the number of Dml records that should be consumed
     * @return the collector to which the records were captured; never null
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected SourceRecords consumeDmlRecordsByTopic(int numDmlRecords) throws InterruptedException {
        SourceRecords records = new SourceRecords();
        consumeDmlRecordsByTopic(numDmlRecords, records::add);
        return records;
    }

    /**
     * Try to consume the specified number of records from the connector, calling the given function for each, and return the
     * actual number of Dml records that were consumed.
     * For slower connectors it is possible to receive no records from the connector at most 3 times in a row
     * till the waiting is terminated.
     *
     * @param numberDmlRecords the number of Dml records that should be consumed
     * @param recordConsumer the function that should be called for each consumed record
     * @return the actual number of Dml records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeDmlRecordsByTopic(int numberDmlRecords, Consumer<SourceRecord> recordConsumer) throws InterruptedException {
        int breakAfterNulls = waitTimeForRecordsAfterNulls();
        return consumeDmlRecordsByTopic(numberDmlRecords, breakAfterNulls, recordConsumer, true);
    }

    /**
     * Try to consume the specified number of records from the connector, calling the given function for each, and return the
     * actual number of Dml records that were consumed.
     *
     * For slower connectors it is possible to receive no records from the connector at most 3 times in a row
     * until the waiting is terminated.  Additionally, while this method will consume and append transaction metadata
     * topic records to the consumer, the returned value only considers Dml records.
     *
     * @param numberOfRecords the number of Dml records that should be consumed
     * @param breakAfterNulls the number of allowed run when no records are consumed
     * @param recordConsumer the function that should be called for each consumed record
     * @param assertRecords true if records serialization should be verified
     * @return the actual number of Dml records that were consumed
     * @throws InterruptedException if the thread was interrupted while waiting for a record to be returned
     */
    protected int consumeDmlRecordsByTopic(int numberOfRecords, int breakAfterNulls, Consumer<SourceRecord> recordConsumer, boolean assertRecords)
            throws InterruptedException {
        int recordsConsumed = 0;
        int nullReturn = 0;
        Set<String> endTransactions = new LinkedHashSet<>();
        while (recordsConsumed < numberOfRecords) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
            if (record != null) {
                nullReturn = 0;
                final Struct value = (Struct) record.value();
                if (isTransactionRecord(record)) {
                    final String status = value.getString(TransactionMonitor.DEBEZIUM_TRANSACTION_STATUS_KEY);
                    final String txId = value.getString(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY);
                    String id = Arrays.stream(txId.split(":")).findFirst().get();
                    if (status.equals(TransactionStatus.BEGIN.name())) {
                        endTransactions.add(id);
                    }
                    else {
                        endTransactions.remove(id);
                    }
                }
                else {
                    final String txId = value.getStruct("source").getInt64("txId").toString();
                    assertThat(endTransactions.contains(txId)).as("DML record txId " + txId + " not in open transaction set").isTrue();
                    ++recordsConsumed;
                }
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
                if (Testing.Debug.isEnabled()) {
                    Testing.debug("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more), " + endTransactions.size() + " active transactions");
                    debug(record);
                }
                else if (Testing.Print.isEnabled()) {
                    Testing.print("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more), " + endTransactions.size() + " active transactions");
                    print(record);
                }
                if (assertRecords) {
                    VerifyRecord.isValid(record);
                }
            }
            else {
                if (++nullReturn >= breakAfterNulls) {
                    return recordsConsumed;
                }
            }
        }

        while (!endTransactions.isEmpty()) {
            SourceRecord record = consumedLines.poll(pollTimeoutInMs, TimeUnit.MILLISECONDS);
            if (record != null) {
                nullReturn = 0;
                final Struct value = (Struct) record.value();
                if (isTransactionRecord(record)) {
                    final String status = value.getString(TransactionMonitor.DEBEZIUM_TRANSACTION_STATUS_KEY);
                    if (status.equals(TransactionStatus.END.name())) {
                        endTransactions.remove(value.getString(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY));
                    }
                    else {
                        endTransactions.add(value.getString(TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY));
                    }
                }
                else {
                    final String txId = value.getStruct("source").getInt64("txId").toString();
                    assertThat(endTransactions.contains(txId)).as("DML record txId " + txId + " not in open transaction set").isTrue();
                    ++recordsConsumed;
                }
                if (recordConsumer != null) {
                    recordConsumer.accept(record);
                }
                if (Testing.Debug.isEnabled()) {
                    Testing.debug("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more), " + endTransactions.size() + " active transactions");
                    debug(record);
                }
                else if (Testing.Print.isEnabled()) {
                    Testing.print("Consumed record " + recordsConsumed + " / " + numberOfRecords + " ("
                            + (numberOfRecords - recordsConsumed) + " more), " + endTransactions.size() + " active transactions");
                    print(record);
                }
                if (assertRecords) {
                    VerifyRecord.isValid(record);
                }
            }
            else {
                if (++nullReturn >= breakAfterNulls) {
                    return recordsConsumed;
                }
            }
        }
        return recordsConsumed;
    }

    protected boolean isTransactionRecord(SourceRecord record) {
        return record != null
                && record.topic().endsWith(".transaction")
                && record.keySchema().name().equals("io.debezium.connector.common.TransactionMetadataKey");
    }

    protected class SourceRecords {
        private final List<SourceRecord> records = new ArrayList<>();
        private final Map<String, List<SourceRecord>> recordsByTopic = new HashMap<>();
        private final Map<String, List<SourceRecord>> ddlRecordsByDbName = new HashMap<>();

        public void add(SourceRecord record) {
            records.add(record);
            recordsByTopic.computeIfAbsent(record.topic(), (topicName) -> new ArrayList<SourceRecord>()).add(record);
            String dbName = getAffectedDatabase(record);
            if (dbName != null) {
                ddlRecordsByDbName.computeIfAbsent(dbName, key -> new ArrayList<>()).add(record);
            }
        }

        protected String getAffectedDatabase(SourceRecord record) {
            Struct value = (Struct) record.value();
            if (value != null) {
                Field dbField = value.schema().field(HistoryRecord.Fields.DATABASE_NAME);
                if (dbField != null) {
                    return value.getString(dbField.name());
                }
            }
            return null;
        }

        /**
         * Get the DDL events for the named database.
         *
         * @param dbName the name of the database; may not be null
         * @return the DDL-related events; never null but possibly empty
         */
        public List<SourceRecord> ddlRecordsForDatabase(String dbName) {
            return ddlRecordsByDbName.get(dbName);
        }

        /**
         * Get the names of the databases that were affected by the DDL statements.
         *
         * @return the set of database names; never null but possibly empty
         */
        public Set<String> databaseNames() {
            return ddlRecordsByDbName.keySet();
        }

        /**
         * Get the records on the given topic.
         *
         * @param topicName the name of the topic.
         * @return the records for the topic; possibly null if there were no records produced on the topic
         */
        public List<SourceRecord> recordsForTopic(String topicName) {
            return recordsByTopic.get(topicName);
        }

        /**
         * Get the set of topics for which records were received.
         *
         * @return the names of the topics; never null
         */
        public Set<String> topics() {
            return recordsByTopic.keySet();
        }

        public void forEachInTopic(String topic, Consumer<SourceRecord> consumer) {
            recordsForTopic(topic).forEach(consumer);
        }

        public void forEach(Consumer<SourceRecord> consumer) {
            records.forEach(consumer);
        }

        public List<SourceRecord> allRecordsInOrder() {
            return Collections.unmodifiableList(records);
        }

        public void print() {
            Testing.print("" + topics().size() + " topics: " + topics());
            recordsByTopic.forEach((k, v) -> {
                Testing.print(" - topic:'" + k + "'; # of events = " + v.size());
            });
            Testing.print("Records:");
            records.forEach(AbstractConnectorTest.this::print);
        }
    }

    /**
     * Try to consume all of the messages that have already been returned by the connector.
     *
     * @param recordConsumer the function that should be called with each consumed record
     * @return the number of records that were consumed
     */
    protected int consumeAvailableRecords(Consumer<SourceRecord> recordConsumer) {
        List<SourceRecord> records = new LinkedList<>();
        consumedLines.drainTo(records);
        if (recordConsumer != null) {
            records.forEach(recordConsumer);
        }
        return records.size();
    }

    /**
     * Wait for a maximum amount of time until the first record is available.
     *
     * @param timeout the maximum amount of time to wait; must not be negative
     * @param unit the time unit for {@code timeout}
     * @return {@code true} if records are available, or {@code false} if the timeout occurred and no records are available
     */
    protected boolean waitForAvailableRecords(long timeout, TimeUnit unit) {
        assertThat(timeout).isGreaterThanOrEqualTo(0);
        long now = System.currentTimeMillis();
        long stop = now + unit.toMillis(timeout);
        while (System.currentTimeMillis() < stop) {
            if (!consumedLines.isEmpty()) {
                break;
            }
        }
        return !consumedLines.isEmpty();
    }

    /**
     * Disable record validation using Avro converter.
     */
    protected void skipAvroValidation() {
        skipAvroValidation = true;
    }

    /**
     * Assert that the connector is currently running.
     */
    protected void assertConnectorIsRunning() {
        assertThat(engine.isRunning()).isTrue();
    }

    /**
     * Assert that the connector is NOT currently running.
     */
    protected void assertConnectorNotRunning() {
        assertThat(engine != null && engine.isRunning()).isFalse();
    }

    /**
     * Assert that there are no records to consume.
     */
    protected void assertNoRecordsToConsume() {
        assertThat(consumedLines.isEmpty()).isTrue();
    }

    /**
     * Assert that there are only transaction topic records to be consumed.
     */
    protected void assertOnlyTransactionRecordsToConsume() {
        consumedLines.iterator().forEachRemaining(r -> assertThat(isTransactionRecord(r)).isTrue());
    }

    protected void assertKey(SourceRecord record, String pkField, int pk) {
        VerifyRecord.hasValidKey(record, pkField, pk);
    }

    protected void assertInsert(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidInsert(record, pkField, pk);
    }

    protected void assertUpdate(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidUpdate(record, pkField, pk);
    }

    protected void assertDelete(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidDelete(record, pkField, pk);
    }

    protected void assertSourceQuery(SourceRecord record, String query) {
        VerifyRecord.hasValidSourceQuery(record, query);
    }

    protected void assertHasNoSourceQuery(SourceRecord record) {
        VerifyRecord.hasNoSourceQuery(record);
    }

    protected void assertTombstone(SourceRecord record, String pkField, int pk) {
        VerifyRecord.isValidTombstone(record, pkField, pk);
    }

    protected void assertTombstone(SourceRecord record) {
        VerifyRecord.isValidTombstone(record);
    }

    protected void assertOffset(SourceRecord record, Map<String, ?> expectedOffset) {
        Map<String, ?> offset = record.sourceOffset();
        assertThat(offset).isEqualTo(expectedOffset);
    }

    protected void assertOffset(SourceRecord record, String offsetField, Object expectedValue) {
        Map<String, ?> offset = record.sourceOffset();
        Object value = offset.get(offsetField);
        assertSameValue(value, expectedValue);
    }

    protected void assertValueField(SourceRecord record, String fieldPath, Object expectedValue) {
        VerifyRecord.assertValueField(record, fieldPath, expectedValue);
    }

    private void assertSameValue(Object actual, Object expected) {
        VerifyRecord.assertSameValue(actual, expected);
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     *
     * @param value the value with a schema; may not be null
     */
    protected void assertSchemaMatchesStruct(SchemaAndValue value) {
        VerifyRecord.schemaMatchesStruct(value);
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     *
     * @param struct the {@link Struct} to validate; may not be null
     * @param schema the expected schema of the {@link Struct}; may not be null
     */
    protected void assertSchemaMatchesStruct(Struct struct, Schema schema) {
        VerifyRecord.schemaMatchesStruct(struct, schema);
    }

    /**
     * Assert that there was no exception in engine that would cause its termination.
     */
    protected void assertEngineIsRunning() {
        assertThat(engine.isRunning()).as("Engine should not fail due to an exception").isTrue();
    }

    /**
     * Validate that a {@link SourceRecord}'s key and value can each be converted to a byte[] and then back to an equivalent
     * {@link SourceRecord}.
     *
     * @param record the record to validate; may not be null
     */
    protected void validate(SourceRecord record) {
        VerifyRecord.isValid(record);
    }

    protected void print(SourceRecord record) {
        VerifyRecord.print(record);
    }

    protected void debug(SourceRecord record) {
        VerifyRecord.debug(record);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field, int numErrors) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isEqualTo(numErrors);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field, int minErrorsInclusive,
                                             int maxErrorsInclusive) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isGreaterThanOrEqualTo(minErrorsInclusive);
        assertThat(value.errorMessages().size()).isLessThanOrEqualTo(maxErrorsInclusive);
    }

    protected void assertConfigurationErrors(Config config, io.debezium.config.Field field) {
        ConfigValue value = configValue(config, field.name());
        assertThat(value.errorMessages().size()).isGreaterThan(0);
    }

    protected void assertNoConfigurationErrors(Config config, io.debezium.config.Field... fields) {
        for (io.debezium.config.Field field : fields) {
            ConfigValue value = configValue(config, field.name());
            if (value != null) {
                if (!value.errorMessages().isEmpty()) {
                    fail("Error messages on field '" + field.name() + "': " + value.errorMessages());
                }
            }
        }
    }

    protected ConfigValue configValue(Config config, String fieldName) {
        return config.configValues().stream().filter(value -> value.name().equals(fieldName)).findFirst().orElse(null);
    }

    /**
     * Utility to read the last committed offset for the specified partition.
     *
     * @param config the configuration of the engine used to persist the offsets
     * @param partition the partition
     * @return the map of partitions to offsets; never null but possibly empty
     */
    protected <T> Map<String, Object> readLastCommittedOffset(Configuration config, Map<String, T> partition) {
        return readLastCommittedOffsets(config, Arrays.asList(partition)).get(partition);
    }

    /**
     * Utility to read the last committed offsets for the specified partitions.
     *
     * @param config the configuration of the engine used to persist the offsets
     * @param partitions the partitions
     * @return the map of partitions to offsets; never null but possibly empty
     */
    protected <T> Map<Map<String, T>, Map<String, Object>> readLastCommittedOffsets(Configuration config,
                                                                                    Collection<Map<String, T>> partitions) {
        config = config.edit().with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH)
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .build();

        final String engineName = config.getString(EmbeddedEngine.ENGINE_NAME);
        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        Converter keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
        keyConverter.configure(internalConverterConfig, true);
        Converter valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
        valueConverter.configure(internalConverterConfig, false);

        // Create the worker config, adding extra fields that are required for validation of a worker config
        // but that are not used within the embedded engine (since the source records are never serialized) ...
        Map<String, String> embeddedConfig = config.asMap(EmbeddedEngine.ALL_FIELDS);
        embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        WorkerConfig workerConfig = new EmbeddedConfig(embeddedConfig);

        FileOffsetBackingStore offsetStore = KafkaConnectUtil.fileOffsetBackingStore();
        offsetStore.configure(workerConfig);
        offsetStore.start();
        try {
            OffsetStorageReaderImpl offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName, keyConverter, valueConverter);
            return offsetReader.offsets(partitions);
        }
        finally {
            offsetStore.stop();
        }
    }

    @SuppressWarnings("unchecked")
    protected String assertBeginTransaction(SourceRecord record) {
        final Struct begin = (Struct) record.value();
        final Struct beginKey = (Struct) record.key();
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

        assertThat(begin.getString("status")).isEqualTo("BEGIN");
        assertThat(begin.getInt64("event_count")).isNull();
        final String txId = begin.getString("id");
        assertThat(beginKey.getString("id")).isEqualTo(txId);

        assertThat(offset.get("transaction_id")).isEqualTo(txId);
        return txId;
    }

    @SuppressWarnings("unchecked")
    protected void assertEndTransaction(SourceRecord record, String expectedTxId, long expectedEventCount, Map<String, Number> expectedPerTableCount) {
        final Struct end = (Struct) record.value();
        final Struct endKey = (Struct) record.key();
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

        assertThat(end.getString("status")).isEqualTo("END");
        assertThat(end.getString("id")).isEqualTo(expectedTxId);
        assertThat(end.getInt64("event_count")).isEqualTo(expectedEventCount);
        assertThat(endKey.getString("id")).isEqualTo(expectedTxId);

        assertThat(end.getArray("data_collections").stream().map(x -> (Struct) x)
                .collect(Collectors.toMap(x -> x.getString("data_collection"), x -> x.getInt64("event_count"))))
                .isEqualTo(expectedPerTableCount.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().longValue())));
        assertThat(offset.get("transaction_id")).isEqualTo(expectedTxId);
    }

    @SuppressWarnings("unchecked")
    protected void assertRecordTransactionMetadata(SourceRecord record, String expectedTxId, long expectedTotalOrder, long expectedCollectionOrder) {
        final Struct change = ((Struct) record.value()).getStruct("transaction");
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

        assertThat(change.getString("id")).isEqualTo(expectedTxId);
        assertThat(change.getInt64("total_order")).isEqualTo(expectedTotalOrder);
        assertThat(change.getInt64("data_collection_order")).isEqualTo(expectedCollectionOrder);
        assertThat(offset.get("transaction_id")).isEqualTo(expectedTxId);
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    public static int waitTimeForRecordsAfterNulls() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime.after.nulls", "3"));
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server) throws InterruptedException {
        waitForSnapshotEvent(connector, server, "SnapshotCompleted", null, null);
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server, String task, String database) throws InterruptedException {
        waitForSnapshotEvent(connector, server, "SnapshotCompleted", task, database);
    }

    private static void waitForSnapshotEvent(String connector, String server, String event, String task, String database) throws InterruptedException {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> (boolean) mbeanServer
                        .getAttribute(getSnapshotMetricsObjectName(connector, server, task, database), event));
    }

    public static void waitForStreamingRunning(String connector, String server) throws InterruptedException {
        waitForStreamingRunning(connector, server, getStreamingNamespace());
    }

    public static void waitForStreamingRunning(String connector, String server, String contextName) {
        waitForStreamingRunning(connector, server, contextName, null);
    }

    public static void waitForStreamingRunning(String connector, String server, String contextName, String task) {
        Awaitility.await()
                .alias("Streaming was not started on time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> isStreamingRunning(connector, server, contextName, task));
    }

    public static void waitForConnectorShutdown(String connector, String server) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(waitTimeForRecords() * 30, TimeUnit.SECONDS)
                .until(() -> !isStreamingRunning(connector, server));
    }

    public static boolean isStreamingRunning(String connector, String server) {
        return isStreamingRunning(connector, server, getStreamingNamespace(), null);
    }

    public static boolean isStreamingRunning(String connector, String server, String contextName) {
        return isStreamingRunning(connector, server, contextName, null);
    }

    public static boolean isStreamingRunning(String connector, String server, String contextName, String task) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName streamingMetricsObjectName = task != null ? getStreamingMetricsObjectName(connector, server, contextName, task)
                    : getStreamingMetricsObjectName(connector, server, contextName);
            return (boolean) mbeanServer.getAttribute(streamingMetricsObjectName, "Connected");
        }
        catch (JMException ignored) {
        }
        return false;
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server);
    }

    public static ObjectName getSnapshotMetricsObjectName(String connector, String server, String task, String database) throws MalformedObjectNameException {

        Map<String, String> props = new HashMap<>();
        props.put("task", task);
        props.put("database", database);
        String additionalProperties = props.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

        if (additionalProperties.length() != 0) {
            return new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server + "," + additionalProperties);
        }

        return getSnapshotMetricsObjectName(connector, server);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server) throws MalformedObjectNameException {
        return getStreamingMetricsObjectName(connector, server, getStreamingNamespace());
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server);
    }

    public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context, String task) throws MalformedObjectNameException {
        return new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server + ",task=" + task);
    }

    protected static String getStreamingNamespace() {
        return System.getProperty("test.streaming.metrics.namespace", "streaming");
    }
}

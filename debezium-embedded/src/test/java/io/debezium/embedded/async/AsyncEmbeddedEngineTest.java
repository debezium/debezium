/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.Connect;
import io.debezium.embedded.ConvertingEngineBuilder;
import io.debezium.embedded.DebeziumEngineTestUtils;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.EmbeddedEngineHeader;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * Tests for {@link AsyncEmbeddedEngine} implementation of {@link DebeziumEngine}.
 *
 * @author vjuranek
 */
public class AsyncEmbeddedEngineTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncEmbeddedEngineTest.class);

    private static final int NUMBER_OF_LINES = 10;
    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();
    private static final Path TEST_FILE_PATH = Testing.Files.createTestingPath("file-connector-input.txt").toAbsolutePath();

    protected static final AtomicBoolean isEngineRunning = new AtomicBoolean(false);
    protected static final AtomicInteger runningTasks = new AtomicInteger(0);
    protected DebeziumEngine<SourceRecord> engine;
    protected ExecutorService engineExecSrv = Executors.newFixedThreadPool(1);

    private File inputFile;
    private int linesAdded;

    @Before
    public void beforeEach() throws Exception {
        linesAdded = 0;
        Testing.Files.delete(TEST_FILE_PATH);
        Testing.Files.delete(OFFSET_STORE_PATH);
        inputFile = Testing.Files.createTestingFile(TEST_FILE_PATH);
        isEngineRunning.set(false);
        runningTasks.set(0);
    }

    @Test
    public void testEngineBasicLifecycle() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");

        appendLinesToSource(NUMBER_OF_LINES);

        CountDownLatch snapshotLatch = new CountDownLatch(1);
        CountDownLatch allLatch = new CountDownLatch(6);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    snapshotLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        snapshotLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(snapshotLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        stopEngine();
    }

    @Test
    public void testRunMultipleTasks() throws Exception {

        final int NUMBER_OF_TASKS = 5;
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(NUMBER_OF_TASKS));
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), DebeziumAsyncEngineTestUtils.MultiTaskSimpleSourceConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);

        final AtomicInteger recordsRead = new AtomicInteger(0);
        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .notifying((records, committer) -> {
                    for (SourceRecord record : records) {
                        recordsRead.incrementAndGet();
                        committer.markProcessed(record);
                    }
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        Awaitility.await()
                .alias("Haven't read all the records in time")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                .until(() -> recordsRead.get() == NUMBER_OF_TASKS * SimpleSourceConnector.DEFAULT_BATCH_COUNT);

        stopEngine();
    }

    @Test
    public void testTasksAreStoppedIfSomeFailsToStart() {
        final int NUMBER_OF_TASKS = 10;
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, String.valueOf(NUMBER_OF_TASKS));
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), DebeziumAsyncEngineTestUtils.AlmostRandomlyFailingDuringStartConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);
        props.put(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, "10");

        final AtomicInteger recordsRead = new AtomicInteger(0);
        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                })
                .using(this.getClass().getClassLoader())
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        // At least some tasks should start.
        Awaitility.await()
                .alias("At least some tasks haven't stared on time")
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                .until(() -> runningTasks.get() > 0);

        // Once some tasks failed to start, all started tasks should be stopped.
        Awaitility.await()
                .alias("Tasks haven't been stopped on time")
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                // if task fails to start, we don't call task callback, and we call stop for all tasks no matter if they started successfully or not
                // therefore it is possible that number of running tasks become negative
                .until(() -> runningTasks.get() <= 0);

        // As some of the tasks failed, engine should be stopped automatically.
        waitForEngineToStop();
    }

    @Test
    public void testHeaderConverter() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty("transforms", "header");
        props.setProperty("transforms.header.type", "io.debezium.embedded.async.FixedValueHeader");

        appendLinesToSource(1);
        CountDownLatch recordsLatch = new CountDownLatch(1); // 1 count down for headers

        DebeziumEngine.Builder<EmbeddedEngineChangeEvent> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>(
                KeyValueHeaderChangeEventFormat.of(Json.class, Json.class, Json.class));
        DebeziumEngine<EmbeddedEngineChangeEvent> embeddedEngine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    for (EmbeddedEngineChangeEvent r : records) {
                        committer.markProcessed(r);
                        assertThat(r.headers().size()).isEqualTo(1);
                        assertThat(
                                ((EmbeddedEngineHeader) r.headers().get(0)).getValue()).isEqualTo("{\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":2}");
                    }
                    committer.markBatchFinished();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            embeddedEngine.run();
        });

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);

        embeddedEngine.close();
    }

    @Test
    public void testCompletionCallbackCalledUponSuccess() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");

        appendLinesToSource(NUMBER_OF_LINES);

        CountDownLatch recordsLatch = new CountDownLatch(2); // 2 count down - one for snapshot batch, one for streaming batch
        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger recordsSent = new AtomicInteger();

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .using((success, message, error) -> {
                    if (success && error == null) {
                        callbackLatch.countDown();
                    }
                })
                .notifying((records, committer) -> {
                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                        recordsSent.getAndIncrement();
                    }
                    committer.markBatchFinished();
                    recordsLatch.countDown();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        // Add a few more lines, and then verify they are consumed ...
        appendLinesToSource(NUMBER_OF_LINES);
        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(recordsSent.get()).isEqualTo(20);

        stopEngine();
        callbackLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(callbackLatch.getCount()).isEqualTo(0);
    }

    @Test
    public void testCompletionCallbackCalledUponFailure() throws Exception {
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), DebeziumAsyncEngineTestUtils.InterruptedConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);
        props.put(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, "10");

        CountDownLatch callbackLatch = new CountDownLatch(1);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .using((success, message, error) -> {
                    // Original exception is wrapped into ExecutionException, so we need to call error.getCause() to get original exception.
                    if (!success && error instanceof InterruptedException) {
                        callbackLatch.countDown();
                    }
                })
                .notifying((records, committer) -> {
                })
                .build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        callbackLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(callbackLatch.getCount()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-8414")
    public void testErrorInConnectorCallbackDoesNotBlockShutdown() throws Exception {
        final Properties props = new Properties();
        props.put(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.put(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.put(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");

        appendLinesToSource(NUMBER_OF_LINES);
        AtomicInteger recordsSent = new AtomicInteger();
        CountDownLatch recordsLatch = new CountDownLatch(1);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback() {
                    @Override
                    public void connectorStopped() {
                        super.connectorStopped();
                        throw new RuntimeException("User connector callback exception, enjoy");
                    }
                })
                .notifying((records, committer) -> {
                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                        recordsSent.getAndIncrement();
                    }
                    committer.markBatchFinished();
                    recordsLatch.countDown();
                }).build();

        // Start engine and make sure it's running.
        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(recordsSent.get()).isEqualTo(NUMBER_OF_LINES);

        // Stop engine in another thread to avoid blocking the main thread if it gets stuck.
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                engine.close();
                shutdownLatch.countDown();
            }
            catch (IOException e) {
                // pass
            }
        });

        // Assert that latch was counted down, i.e. closing the engine hasn't blocked forever.
        shutdownLatch.await(1, TimeUnit.SECONDS);
        assertThat(shutdownLatch.getCount()).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-2534")
    public void testCannotStopWhileTasksAreStarting() throws Exception {
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), AsyncEmbeddedEngineTest.WaitInTaskStartConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);
        props.put(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, "10");

        CountDownLatch taskStartingLatch = new CountDownLatch(1);
        CountDownLatch enginStopLatch = new CountDownLatch(1);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                })
                .build();
        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        WaitInTaskStartTask.taskStartingLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);

        Exception error = null;
        try {
            stopEngine();
        }
        catch (Exception e) {
            error = e;
        }
        assertThat(error).isNotNull();
        assertThat(error).isInstanceOf(IllegalStateException.class);
        assertThat(error.getMessage())
                .isEqualTo("Cannot stop engine while tasks are starting, this may lead to leaked resource. Wait for the tasks to be fully started.");

        WaitInTaskStartTask.continueLatch.countDown();
        waitForTasksToStart(1);
        stopEngine();
    }

    @Test
    public void testCannotStopAlreadyStoppedEngine() throws Exception {
        final Properties props = new Properties();
        props.put(EmbeddedEngineConfig.ENGINE_NAME.name(), "testing-connector");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put(EmbeddedEngineConfig.CONNECTOR_CLASS.name(), DebeziumAsyncEngineTestUtils.NoOpConnector.class.getName());
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.put(SimpleSourceConnector.BATCH_COUNT, 1);
        props.put(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, "10");

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                })
                .build();
        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        waitForTasksToStart(1);

        stopEngine();
        waitForEngineToStop();

        Exception error = null;
        try {
            stopEngine();
        }
        catch (Exception e) {
            error = e;
        }
        assertThat(error).isNotNull();
        assertThat(error).isInstanceOf(IllegalStateException.class);
        assertThat(error.getMessage()).isEqualTo("Engine has been already shut down.");
    }

    @Test
    public void testExecuteSmt() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty("predicates", "filter");
        props.setProperty("predicates.filter.type", DebeziumEngineTestUtils.FilterPredicate.class.getName());
        props.setProperty("transforms", "filter, router");
        props.setProperty("transforms.filter.type", "io.debezium.embedded.DebeziumEngineTestUtils$FilterTransform");
        props.setProperty("transforms.filter.predicate", "filter");
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.RegexRouter");
        props.setProperty("transforms.router.regex", "(.*)");
        props.setProperty("transforms.router.replacement", "routing_smt_$1");

        appendLinesToSource(NUMBER_OF_LINES);

        CountDownLatch snapshotLatch = new CountDownLatch(1);
        // We have only 5 groups as the first one is filtered out (first records is filtered out and therefore group not counted)
        CountDownLatch allLatch = new CountDownLatch(5);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    // The first event is filtered out.
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES - 1);

                    records.forEach(r -> assertThat(r.topic()).isEqualTo("routing_smt_testTopic"));
                    Integer groupCount = records.size() / NUMBER_OF_LINES;

                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                    }

                    committer.markBatchFinished();
                    snapshotLatch.countDown();
                    for (int i = 0; i < groupCount; i++) {
                        allLatch.countDown();
                    }
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        snapshotLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(snapshotLatch.getCount()).isEqualTo(0);

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        stopEngine();
    }

    @Test
    @FixFor("DBZ-8106")
    public void testCloseSmt() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty("transforms", "close");
        props.setProperty("transforms.close.type", "io.debezium.embedded.async.AsyncEmbeddedEngineTest$CloseTestTransform");

        CountDownLatch callbackLatch = new CountDownLatch(1);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .using((success, message, error) -> {
                    if (success) {
                        callbackLatch.countDown();
                    }
                })
                .notifying((records, committer) -> {
                })
                .build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        callbackLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        stopEngine();

        assertThat(CloseTestTransform.wasClosed).isTrue();
    }

    @Test
    public void testPollingIsRetriedUponFailure() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SimpleSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(SimpleSourceConnector.RETRIABLE_ERROR_ON, "5, 7");

        CountDownLatch recordsLatch = new CountDownLatch(SimpleSourceConnector.DEFAULT_BATCH_COUNT);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    assertThat(records.size()).isEqualTo(SimpleSourceConnector.DEFAULT_RECORD_COUNT_PER_BATCH);
                    committer.markProcessed(records.get(0));
                    committer.markBatchFinished();
                    recordsLatch.countDown();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(recordsLatch.getCount()).isEqualTo(0);

        stopEngine();
    }

    @Test
    public void testConnectorFailsIfMaxRetriesExceeded() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SimpleSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(SimpleSourceConnector.RETRIABLE_ERROR_ON, "5, 7");
        props.setProperty(EmbeddedEngineConfig.ERRORS_MAX_RETRIES.name(), "1");

        CountDownLatch recordsLatch = new CountDownLatch(SimpleSourceConnector.DEFAULT_BATCH_COUNT);
        final LogInterceptor interceptor = new LogInterceptor(AsyncEmbeddedEngine.class);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    assertThat(records.size()).isEqualTo(SimpleSourceConnector.DEFAULT_RECORD_COUNT_PER_BATCH);
                    committer.markProcessed(records.get(0));
                    committer.markBatchFinished();
                    recordsLatch.countDown();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        // Engine should fail on record 7 as we have only one retry.
        assertThat(recordsLatch.getCount()).isEqualTo(4);

        waitForEngineToStop();
        // Engine failed with an error.
        assertThat(interceptor.containsErrorMessage("Engine has failed with")).isTrue();
        // Engine was stopped without stop() begin explicitly called.
        assertThat(interceptor.containsMessage("Engine state has changed from 'POLLING_TASKS' to 'STOPPING'")).isTrue();
        // And engine was successfully stopped.
        assertThat(interceptor.containsMessage("Engine state has changed from 'STOPPING' to 'STOPPED'")).isTrue();
    }

    @Test
    public void testEngineBasicLifecycleConsumerSequentially() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_ORDER.name(), "ORDERED");

        runEngineBasicLifecycleWithConsumer(props);
    }

    @Test
    public void testEngineBasicLifecycleConsumerNonSequentially() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_ORDER.name(), "UNORDERED");

        runEngineBasicLifecycleWithConsumer(props);
    }

    @Test
    @FixFor("DBZ-7496")
    public void testCompletionCallbackCalledAfterConnectorStop() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");

        appendLinesToSource(NUMBER_OF_LINES);

        CountDownLatch recordsLatch = new CountDownLatch(2); // 2 count down - one for snapshot batch, one for streaming batch
        CountDownLatch completionCallbackLatch = new CountDownLatch(1);
        AtomicInteger recordsSent = new AtomicInteger();
        AtomicBoolean connectorCallbackCalled = new AtomicBoolean(false);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using((success, message, error) -> {
                    if (success && error == null) {
                        assertThat(connectorCallbackCalled.get()).isTrue();
                        completionCallbackLatch.countDown();
                    }
                })
                .notifying((records, committer) -> {
                    for (SourceRecord r : records) {
                        committer.markProcessed(r);
                        recordsSent.getAndIncrement();
                    }
                    committer.markBatchFinished();
                    recordsLatch.countDown();
                })
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override
                    public void connectorStarted() {
                        isEngineRunning.compareAndExchange(false, true);
                    }

                    @Override
                    public void connectorStopped() {
                        try {
                            Thread.sleep(1_000); // sleep 1 second to make sure we don't return too early
                        }
                        catch (InterruptedException e) {
                            LOGGER.warn("Connector callback was interrupted.");
                        }
                        connectorCallbackCalled.set(true);
                        isEngineRunning.set(false);
                    }
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        waitForEngineToStart();
        LOGGER.info("Stopping engine");
        engine.close();
        // If assertThat(connectorCallbackCalled.get()).isTrue() in completion callback throws, we will time out here.
        completionCallbackLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(completionCallbackLatch.getCount()).isEqualTo(0);
        assertThat(connectorCallbackCalled.get()).isTrue();
    }

    @Test
    @FixFor("DBZ-8241")
    public void testDefaultBuilderFactory() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");

        DebeziumEngine.Builder<ChangeEvent<SourceRecord, SourceRecord>> builder = DebeziumEngine.create(Connect.class);

        // Until EmbeddedEngine is removed, the default is EmbeddedEngine.
        assertThat(builder).isInstanceOf(ConvertingEngineBuilder.class);

        // Verify that engine created by default builder factory works.
        appendLinesToSource(NUMBER_OF_LINES);
        CountDownLatch recordsLatch = new CountDownLatch(NUMBER_OF_LINES);

        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying((records, committer) -> {
                    assertThat(records.size()).isGreaterThanOrEqualTo(NUMBER_OF_LINES);
                    for (int i = 0; i < records.size(); i++) {
                        assertThat(records.get(i).value()).isInstanceOf(SourceRecord.class);
                        recordsLatch.countDown();
                    }
                    committer.markBatchFinished();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(recordsLatch.getCount()).isEqualTo(0);

        engine.close();
    }

    @Test
    @FixFor("DBZ-8434")
    public void testSmtReturnsNullToProcessor() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty("transforms", "null");
        props.setProperty("transforms.null.type", "io.debezium.embedded.async.AsyncEmbeddedEngineTest$OddIsNullTransform");

        final int numRecords = NUMBER_OF_LINES / 2;
        CountDownLatch recordsLatch = new CountDownLatch(numRecords);
        AtomicBoolean receivedNull = new AtomicBoolean(false);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying(record -> {
                    if (record == null) {
                        receivedNull.set(true);
                    }
                    else {
                        recordsLatch.countDown();
                    }
                })
                .build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
        appendLinesToSource(numRecords * 2);

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        stopEngine();

        assertThat(recordsLatch.getCount()).isEqualTo(0);
        assertThat(receivedNull.get()).isFalse();
    }

    @Test
    @FixFor("DBZ-8434")
    public void testSmtReturnsNullToProcessorAndConvertor() throws Exception {
        final Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "debezium-engine");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        props.setProperty(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "0");
        props.setProperty(FileStreamSourceConnector.FILE_CONFIG, TEST_FILE_PATH.toAbsolutePath().toString());
        props.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, "testTopic");
        props.setProperty("transforms", "null");
        props.setProperty("transforms.null.type", "io.debezium.embedded.async.AsyncEmbeddedEngineTest$OddIsNullTransform");

        final int numRecords = NUMBER_OF_LINES / 2;
        CountDownLatch recordsLatch = new CountDownLatch(numRecords);
        AtomicBoolean receivedNull = new AtomicBoolean(false);

        DebeziumEngine.Builder<EmbeddedEngineChangeEvent> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>(
                KeyValueHeaderChangeEventFormat.of(Json.class, Json.class, Json.class));
        DebeziumEngine<EmbeddedEngineChangeEvent> embeddedEngine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying(record -> {
                    if (record == null) {
                        receivedNull.set(true);
                    }
                    else {
                        recordsLatch.countDown();
                    }
                })
                .build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            embeddedEngine.run();
        });
        appendLinesToSource(numRecords * 2);

        recordsLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        embeddedEngine.close();

        assertThat(recordsLatch.getCount()).isEqualTo(0);
        assertThat(receivedNull.get()).isFalse();
    }

    private void runEngineBasicLifecycleWithConsumer(final Properties props) throws IOException, InterruptedException {

        final LogInterceptor interceptor = new LogInterceptor(AsyncEmbeddedEngine.class);
        interceptor.setLoggerLevel(AsyncEmbeddedEngine.class, Level.DEBUG);

        appendLinesToSource(NUMBER_OF_LINES);
        CountDownLatch allLatch = new CountDownLatch(6 * NUMBER_OF_LINES);

        DebeziumEngine.Builder<SourceRecord> builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<>();
        engine = builder
                .using(props)
                .using(new TestEngineConnectorCallback())
                .notifying(r -> {
                    allLatch.countDown();
                }).build();

        engineExecSrv.submit(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        for (int i = 0; i < 5; i++) {
            // Add a few more lines, and then verify they are consumed ...
            appendLinesToSource(NUMBER_OF_LINES);
            Thread.sleep(10);
        }
        allLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
        assertThat(allLatch.getCount()).isEqualTo(0);

        assertThat(interceptor.containsMessage("Using io.debezium.embedded.async.AsyncEmbeddedEngine$ParallelSmtConsumerProcessor processor"));

        stopEngine();
    }

    protected void stopEngine() {
        try {
            LOGGER.info("Stopping engine");
            engine.close();
            Awaitility.await().atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        }
        catch (IOException e) {
            LOGGER.warn("Failed during engine stop", e);
            engineExecSrv.shutdownNow();
        }
        catch (ConditionTimeoutException e) {
            LOGGER.warn("Engine has not stopped on time");
            engineExecSrv.shutdownNow();
        }
    }

    protected void waitForEngineToStart() {
        Awaitility.await()
                .alias("Engine haven't started on time")
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                .until(() -> isEngineRunning.get());
    }

    protected void waitForEngineToStop() {
        Awaitility.await()
                .alias("Engine haven't stopped on time")
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                .until(() -> !isEngineRunning.get());
    }

    protected void waitForTasksToStart(int minRunningTasks) {
        Awaitility.await()
                .alias("Engine haven't started on time")
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS)
                .until(() -> runningTasks.get() >= minRunningTasks);
    }

    protected void appendLinesToSource(int numberOfLines) throws IOException {
        linesAdded += DebeziumEngineTestUtils.appendLinesToSource(inputFile, numberOfLines, linesAdded);
    }

    public static class TestEngineConnectorCallback implements DebeziumEngine.ConnectorCallback {
        @Override
        public void taskStarted() {
            runningTasks.incrementAndGet();
        }

        @Override
        public void taskStopped() {
            runningTasks.decrementAndGet();
        }

        @Override
        public void connectorStarted() {
            isEngineRunning.compareAndExchange(false, true);
        }

        @Override
        public void connectorStopped() {
            isEngineRunning.set(false);
        }
    }

    static class WaitInTaskStartConnector extends SimpleSourceConnector {

        @Override
        public Class<? extends Task> taskClass() {
            return AsyncEmbeddedEngineTest.WaitInTaskStartTask.class;
        }
    }

    static class WaitInTaskStartTask extends SimpleSourceConnector.SimpleConnectorTask {

        public static CountDownLatch taskStartingLatch = new CountDownLatch(1);
        public static CountDownLatch continueLatch = new CountDownLatch(1);

        @Override
        public void start(Map<String, String> props) {
            taskStartingLatch.countDown();
            try {
                continueLatch.await(AbstractConnectorTest.waitTimeForEngine(), TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new DebeziumException("Waiting for continuation of start was interrupted.");
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            return new ArrayList<SourceRecord>();
        }
    }

    public static class CloseTestTransform implements Transformation<SourceRecord> {

        public static boolean wasClosed;

        @Override
        public SourceRecord apply(SourceRecord record) {
            // Nothing to do.
            return null;
        }

        @Override
        public ConfigDef config() {
            // Nothing to do.
            return null;
        }

        @Override
        public void close() {
            wasClosed = true;
        }

        @Override
        public void configure(Map<String, ?> map) {
            wasClosed = false;
        }
    }

    public static class OddIsNullTransform implements Transformation<SourceRecord> {

        public static int counter = 0;

        @Override
        public SourceRecord apply(SourceRecord record) {
            return (++counter % 2 == 0) ? record : null;
        }

        @Override
        public ConfigDef config() {
            // Nothing to do.
            return null;
        }

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public void configure(Map<String, ?> map) {
            // Nothing to do.
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.connector.simple.FailOnTaskStartupSourceConnector;
import io.debezium.connector.simple.FailTaskExecutionSourceConnector;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.consumer.ChangeEvent;
import io.debezium.embedded.ConnectorEngine.ConnectorCallback;
import io.debezium.util.Testing;
import io.debezium.util.VariableLatch;

/**
 * @author Randall Hauch
 *
 */
public class ConnectorEngineTest {

    protected static final String CONNECTOR1 = "connector1";
    protected static final String CONNECTOR2 = "connector2";
    protected static final String CONNECTOR3 = "connector3";
    protected static final String CONNECTOR4 = "connector4";
    protected static final String CONNECTOR_FAIL_START = "connectorFailStart";
    protected static final String CONNECTOR_FAIL_EXEC = "connectorFailExec";
    protected static final String TOPIC1 = SimpleSourceConnector.TOPIC_NAME.defaultValueAsString();
    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("offsets.txt").toAbsolutePath();

    private ConnectorEngine engine;
    private ConnectorCallbacks callback;

    @Before
    public void beforeEach() {
        Testing.Files.delete(OFFSET_STORE_PATH);
        OFFSET_STORE_PATH.getParent().toFile().mkdirs();

        Configuration config = Configuration.create()
                                            .with(ConnectorEngine.NAME, "test-engine")
                                            .with(ConnectorEngine.OFFSET_STORAGE_FILE_FILENAME, OFFSET_STORE_PATH)
                                            .build();
        engine = new ConnectorEngine(config);
        callback = new ConnectorCallbacks();
        Testing.Print.enable();
    }

    @After
    public void afterEach() throws Exception {
        if (engine != null) {
            try {
                engine.close();
            } finally {
                engine = null;
            }
        }
    }

    @Test
    public void shouldStartAndCloseWithNoConnectorDeployments() throws Exception {
        assertThat(engine.isRunning()).isFalse();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        engine.close();
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    public void shouldDeployConnectorBeforeStarting() throws Exception {
        assertThat(engine.isRunning()).isFalse();
        engine.addConnector(configSimpleConnector(CONNECTOR1, 1), callback);
        assertThat(engine.isRunning()).isFalse();
        callback.assertNoCallbacks();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        callback.forConnector(CONNECTOR1).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStarted(1, 1);
        engine.close();
        callback.forConnector(CONNECTOR1).waitForStopOrFail(3, TimeUnit.SECONDS);
        assertThat(engine.isRunning()).isFalse();
        callback.forConnector(CONNECTOR1).assertStopped(1, 1);
    }

    @Test
    public void shouldDeployConnectorAfterStarting() throws Exception {
        // Start the engine ...
        assertThat(engine.isRunning()).isFalse();
        callback.assertNoCallbacks();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        callback.assertNoCallbacks();

        // Deploy the connector ...
        engine.addConnector(configSimpleConnector(CONNECTOR1, 1), callback);
        callback.forConnector(CONNECTOR1).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStarted(1, 1);

        // Consume all records ...
        AtomicInteger expectedId = new AtomicInteger(1);
        while (true) {
            List<ChangeEvent> events = engine.poll(100, TimeUnit.MILLISECONDS);
            if (events.isEmpty()) break;
            events.forEach(event->{
                assertThat(event.topic()).isEqualTo(TOPIC1);
                assertThat(event.key().getInt32("id")).isEqualTo(expectedId.getAndIncrement());
                assertThat(event.value().get("batch")).isNotNull();
                assertThat(event.value().get("record")).isNotNull();
                event.commit();
            });
        }
        
        // Stop the connector ...
        engine.stopConnector(CONNECTOR1);
        callback.forConnector(CONNECTOR1).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStopped(1, 1);

        // Start the connector again ...
        engine.startConnector(CONNECTOR1);
        callback.forConnector(CONNECTOR1).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStarted(2, 2);
        
        // Verify all records continue where we left off ...
        while (true) {
            List<ChangeEvent> events = engine.poll(100, TimeUnit.MILLISECONDS);
            if (events.isEmpty()) break;
            events.forEach(event->{
                assertThat(event.topic()).isEqualTo(TOPIC1);
                assertThat(event.key().getInt32("id")).isEqualTo(expectedId.getAndIncrement());
                assertThat(event.value().get("batch")).isNotNull();
                assertThat(event.value().get("record")).isNotNull();
                event.commit();
            });
        }
        
        // Stop and remove the connector ...
        engine.removeConnector(CONNECTOR1);

        // Close the engine ...
        engine.close();
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    public void shouldDeployConnectorsAfterStarting() throws Exception {
        // Start the engine ...
        assertThat(engine.isRunning()).isFalse();
        callback.assertNoCallbacks();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        callback.assertNoCallbacks();

        // Deploy connector 1 ...
        engine.addConnector(configSimpleConnector(CONNECTOR1, 1), callback);
        callback.forConnector(CONNECTOR1).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStarted(1, 1);

        // Deploy connector 2 ...
        engine.addConnector(configSimpleConnector(CONNECTOR2, 1), callback);
        callback.forConnector(CONNECTOR2).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR2).assertStarted(1, 1);

        // Stop connector 1 ...
        engine.stopConnector(CONNECTOR1);
        callback.forConnector(CONNECTOR1).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR1).assertStopped(1, 1);

        // Deploy connector 3 ...
        engine.addConnector(configSimpleConnector(CONNECTOR3, 1), callback);
        callback.forConnector(CONNECTOR3).waitForStart(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR3).assertStarted(1, 1);

        // Stop connector 2 ...
        engine.stopConnector(CONNECTOR2);
        callback.forConnector(CONNECTOR2).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR2).assertStopped(1, 1);

        // Stop connector 3 ...
        engine.stopConnector(CONNECTOR3);
        callback.forConnector(CONNECTOR3).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR3).assertStopped(1, 1);

        // Remove connectors ...
        engine.removeAllConnectors().get();
        engine.close();
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    public void shouldHandleRunningConnectorThatFailsDuringStartupOfSoleTask() throws Exception {
        // Start the engine ...
        assertThat(engine.isRunning()).isFalse();
        callback.assertNoCallbacks();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        callback.assertNoCallbacks();

        // Try but fail to run the connector ...
        engine.addConnector(configFailingTaskStartupConnector(CONNECTOR_FAIL_START, 1), callback);
        callback.forConnector(CONNECTOR_FAIL_START).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR_FAIL_START).assertFailed(1, 0);

        // Try again ...
        engine.startConnector(CONNECTOR_FAIL_START);
        callback.forConnector(CONNECTOR_FAIL_START).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR_FAIL_START).assertFailed(2, 0);
        
        assertThat(engine.allConnectorNames()).containsOnly(CONNECTOR_FAIL_START);
        assertThat(engine.runningConnectorNames()).isEmpty();
        
        // Remove the connector ...
        engine.removeConnector(CONNECTOR_FAIL_START);
        engine.close();
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    public void shouldHandleRunningConnectorThatFailsDuringStartupOfOneOfSeveralTasks() throws Exception {
        // Start the engine ...
        assertThat(engine.isRunning()).isFalse();
        callback.assertNoCallbacks();
        engine.start();
        assertThat(engine.isRunning()).isTrue();
        callback.assertNoCallbacks();

        // Try but fail to run the connector ...
        engine.addConnector(configFailingTaskStartupConnector(CONNECTOR_FAIL_START, 3), callback);
        callback.forConnector(CONNECTOR_FAIL_START).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR_FAIL_START).assertFailed(1, 2);

        // Try again ...
        engine.startConnector(CONNECTOR_FAIL_START);
        callback.forConnector(CONNECTOR_FAIL_START).waitForStopOrFail(3, TimeUnit.SECONDS);
        callback.forConnector(CONNECTOR_FAIL_START).assertFailed(2, 4);
        
        assertThat(engine.allConnectorNames()).containsOnly(CONNECTOR_FAIL_START);
        assertThat(engine.runningConnectorNames()).isEmpty();
        
        // Remove the connector ...
        engine.removeConnector(CONNECTOR_FAIL_START);
        engine.close();
        assertThat(engine.isRunning()).isFalse();
    }

    protected static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    protected Configuration configSimpleConnector(String name, int maxTasks) {
        return Configuration.create()
                            .with(ConnectorEngine.CONNECTOR_NAME, name)
                            .with(ConnectorEngine.CONNECTOR_CLASS, SimpleSourceConnector.class.getName())
                            .with(ConnectorEngine.CONNECTOR_TASKS_MAX, maxTasks)
                            .with(SimpleSourceConnector.TASK_COUNT, maxTasks)
                            .build();

    }

    protected Configuration configFailingTaskStartupConnector(String name, int maxTasks) {
        return Configuration.create()
                            .with(ConnectorEngine.CONNECTOR_NAME, name)
                            .with(ConnectorEngine.CONNECTOR_CLASS, FailOnTaskStartupSourceConnector.class.getName())
                            .with(ConnectorEngine.CONNECTOR_TASKS_MAX, maxTasks)
                            .with(FailOnTaskStartupSourceConnector.TASK_COUNT, maxTasks)
                            .build();

    }

    protected Configuration configFailingTaskExecutionConnector(String name, int maxTasks) {
        return Configuration.create()
                            .with(ConnectorEngine.CONNECTOR_NAME, name)
                            .with(ConnectorEngine.CONNECTOR_CLASS, FailTaskExecutionSourceConnector.class.getName())
                            .with(ConnectorEngine.CONNECTOR_TASKS_MAX, maxTasks)
                            .with(FailTaskExecutionSourceConnector.TASK_COUNT, maxTasks)
                            .build();

    }

    protected static void assertNotRunning(ConnectorLifecycleStats stats, int connectorStarts, int connectorStops,
                                           boolean connectorRunning) {

    }

    @Immutable
    protected static class Failure {
        private final String message;
        private final Throwable error;

        public Failure(String message, Throwable error) {
            this.message = message;
            this.error = error;
        }

        public Throwable error() {
            return error;
        }

        public String message() {
            return message;
        }

        @Override
        public String toString() {
            return message + ": " + error.getMessage();
        }
    }

    @ThreadSafe
    public static class ConnectorLifecycleStats implements ConnectorCallback {
        private final String name;
        private final AtomicInteger taskStarts = new AtomicInteger();
        private final AtomicInteger taskStops = new AtomicInteger();
        private final AtomicInteger tasksRunning = new AtomicInteger();
        private final AtomicInteger connectorStarts = new AtomicInteger();
        private final AtomicInteger connectorStops = new AtomicInteger();
        private final List<Failure> connectorFailures = new CopyOnWriteArrayList<>();
        private final AtomicBoolean connectorRunning = new AtomicBoolean();
        private final VariableLatch startLatch = new VariableLatch(1);
        private final VariableLatch stopOrFailLatch = new VariableLatch(0);
        private final VariableLatch allTasksStartedLatch = new VariableLatch(1);
        private final VariableLatch connectorAndTasksStoppedLatch = new VariableLatch(0);

        public ConnectorLifecycleStats(String connectorName) {
            this.name = connectorName;
        }

        public String name() {
            return name;
        }

        @Override
        public void connectorStarted(String name) {
            if (this.name.equals(name)) {
                this.connectorStarts.incrementAndGet();
                this.connectorRunning.set(true);
                this.stopOrFailLatch.countUp();
                this.startLatch.countDown();
                this.connectorAndTasksStoppedLatch.countUp();
            }
        }

        @Override
        public void connectorFailed(String name, String message, Throwable error) {
            if (this.name.equals(name)) {
                this.connectorFailures.add(new Failure(message, error));
                this.connectorRunning.set(false);
                this.stopOrFailLatch.countDown();
                this.startLatch.countUp();
                this.allTasksStartedLatch.countUp();
                this.connectorAndTasksStoppedLatch.countDown();
            }
        }

        @Override
        public void connectorStopped(String name) {
            if (this.name.equals(name)) {
                this.connectorStops.incrementAndGet();
                this.connectorRunning.set(false);
                this.stopOrFailLatch.countDown();
                this.startLatch.countUp();
                this.allTasksStartedLatch.countUp();
                this.connectorAndTasksStoppedLatch.countDown();
            }
        }

        @Override
        public void taskStarted(String name, int taskNumber, int totalTaskCount) {
            if (this.name.equals(name)) {
                this.taskStarts.incrementAndGet();
                this.tasksRunning.incrementAndGet();
                if (taskNumber == totalTaskCount) {
                    allTasksStartedLatch.countDown();
                }
                this.connectorAndTasksStoppedLatch.countUp();
            }
        }

        @Override
        public void taskStopped(String name, int taskNumber, int totalTaskCount) {
            if (this.name.equals(name)) {
                this.taskStops.incrementAndGet();
                this.tasksRunning.decrementAndGet();
                this.connectorAndTasksStoppedLatch.countDown();
            }
        }

        public int runningTaskCount() {
            return this.tasksRunning.get();
        }

        public int taskStarts() {
            return this.taskStarts.get();
        }

        public int taskStops() {
            return this.taskStops.get();
        }

        public boolean isRunning() {
            return this.connectorRunning.get();
        }

        public int connectorStarts() {
            return this.connectorStarts.get();
        }

        public int connectorStops() {
            return this.connectorStops.get();
        }

        public List<Failure> connectorFailures() {
            return Collections.unmodifiableList(this.connectorFailures);
        }

        public void assertConnectorStarts(int connectorStarts) {
            assertThat(connectorStarts()).isEqualTo(connectorStarts);
        }

        public void assertConnectorStops(int connectorStops) {
            assertThat(connectorStops()).isEqualTo(connectorStops);
        }

        public void assertConnectorFailures(int failureCount) {
            if (connectorFailures().size() != failureCount) {
                Testing.print("Connector '" + name + "' failures: " + connectorFailures());
                assertThat(connectorFailures().size()).isEqualTo(failureCount);
            }
        }

        public void assertTaskStarts(int taskStarts) {
            assertThat(taskStarts()).isEqualTo(taskStarts);
        }

        public void assertTaskStops(int taskStops) {
            assertThat(taskStops()).isEqualTo(taskStops);
        }

        public void assertNoCallbacks() {
            assertConnectorStarts(0);
            assertConnectorStops(0);
            assertConnectorFailures(0);
            assertTaskStarts(0);
            assertTaskStops(0);
        }

        public void assertStats(int starts, int stops, int taskStarts, int taskStops) {
            assertConnectorStarts(starts);
            assertConnectorFailures(0);
            assertConnectorStops(stops);
            assertTaskStarts(taskStarts);
            assertTaskStops(taskStops);
            assertThat(stopOrFailLatch.getCount()).isEqualTo(1);
        }
        
        public void assertStarted(int starts, int taskCount) {
            assertConnectorStarts(starts);
            assertTaskStarts(taskCount);
            assertThat(stopOrFailLatch.getCount()).isEqualTo(1);
        }
        
        public void assertStopped(int starts, int taskCount) {
            assertConnectorStarts(starts);
            assertConnectorStops(starts);
            assertConnectorFailures(0);
            assertTaskStarts(taskCount);
            assertTaskStops(taskCount);
            assertThat(stopOrFailLatch.getCount()).isEqualTo(0);
        }

        public void assertFailed(int startFailures, int startedTasks) {
            assertConnectorFailures(startFailures);
            assertTaskStarts(startedTasks);
            assertTaskStops(startedTasks);
            assertThat(stopOrFailLatch.getCount()).isEqualTo(0);
        }

        public void waitForStopOrFail(long timeout, TimeUnit unit) throws InterruptedException {
            stopOrFailLatch.await(timeout, unit);
            connectorAndTasksStoppedLatch.await(timeout, unit);
        }

        public void waitForStart(long timeout, TimeUnit unit) throws InterruptedException {
            startLatch.await(timeout, unit);
            allTasksStartedLatch.await(timeout, unit);
        }

        @Override
        public String toString() {
            return "Connector '" + name + "' (running=" + isRunning() + "; tasks=" + runningTaskCount() + "; starts=" + connectorStarts()
                    + "; stops=" + connectorStops() + "; failures=" + connectorFailures().size() + "; taskStarts=" + taskStarts()
                    + "; taskStops=" + taskStops() + ")";
        }
    }

    @ThreadSafe
    public static class ConnectorCallbacks implements ConnectorCallback {
        private final ConcurrentMap<String, ConnectorLifecycleStats> stats = new ConcurrentHashMap<>();

        @Override
        public void connectorStarted(String name) {
            statsFor(name).connectorStarted(name);
        }

        @Override
        public void connectorFailed(String name, String message, Throwable error) {
            statsFor(name).connectorFailed(name, message, error);
        }

        @Override
        public void connectorStopped(String name) {
            statsFor(name).connectorStopped(name);
        }

        @Override
        public void taskStarted(String name, int taskNumber, int totalTaskCount) {
            statsFor(name).taskStarted(name, taskNumber, totalTaskCount);
        }

        @Override
        public void taskStopped(String name, int taskNumber, int totalTaskCount) {
            statsFor(name).taskStopped(name, taskNumber, totalTaskCount);
        }

        public ConnectorLifecycleStats forConnector(String connectorName) {
            return statsFor(connectorName);
        }

        public void assertNoCallbacks() {
            assertThat(stats.isEmpty()).isTrue();
        }

        protected ConnectorLifecycleStats statsFor(String connectorName) {
            return stats.computeIfAbsent(connectorName, ConnectorLifecycleStats::new);
        }

        @Override
        public String toString() {
            return stats.values()
                        .stream()
                        .map(ConnectorLifecycleStats::toString)
                        .collect(Collectors.joining(System.lineSeparator()));
        }
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.embedded.ConnectorEngine.ConnectorCallback;
import io.debezium.util.VariableLatch;

/**
 * @author Randall Hauch
 *
 */
public class ConnectorCallbacks {

    public static ConnectorCallback loggingCallback(Logger logger) {
        return new ConnectorCallback() {
            @Override
            public void connectorStarted(String name) {
                    logger.info("Connector '{}' started", name);
            }
            @Override
            public void connectorStopped(String name) {
                logger.info("Connector '{}' stopped", name);
            }
            @Override
            public void connectorFailed(String name, String message, Throwable error) {
                logger.error("Connector '{}' failed: {}", name, message, error);
            }
            @Override
            public void taskStarted(String name, int taskNumber, int totalTaskCount) {
                logger.info("Connector '{}' task {}/{} started", name, taskNumber, totalTaskCount);
            }
            @Override
            public void taskStopped(String name, int taskNumber, int totalTaskCount) {
                logger.info("Connector '{}' task {}/{} stopped", name, taskNumber, totalTaskCount);
            }
        };
    }
    
    public static ConnectorResults results() {
        return new ConnectorResults();
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
    public static class ConnectorResult implements ConnectorCallback {
        private final String name;
        protected final AtomicInteger taskStarts = new AtomicInteger();
        protected final AtomicInteger taskStops = new AtomicInteger();
        protected final AtomicInteger tasksRunning = new AtomicInteger();
        protected final AtomicInteger connectorStarts = new AtomicInteger();
        protected final AtomicInteger connectorStops = new AtomicInteger();
        protected final List<Failure> connectorFailures = new CopyOnWriteArrayList<>();
        protected final AtomicBoolean connectorRunning = new AtomicBoolean();
        protected final VariableLatch startLatch = new VariableLatch(1);
        protected final VariableLatch stopOrFailLatch = new VariableLatch(0);
        protected final VariableLatch allTasksStartedLatch = new VariableLatch(1);
        protected final VariableLatch connectorAndTasksStoppedLatch = new VariableLatch(0);

        public ConnectorResult(String connectorName) {
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

        public int connectorFailureCount() {
            return connectorFailures().size();
        }
        
        public boolean hasStartedAndStopped() {
            return this.connectorStarts() == this.connectorStops() && this.connectorStops() > 0;
        }
        
        public boolean hasFailed() {
            return connectorFailureCount() > 0;
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
    public static class ConnectorResults implements ConnectorCallback {
        private final ConcurrentMap<String, ConnectorResult> stats = new ConcurrentHashMap<>();

        @Override
        public void connectorStarted(String name) {
            resultFor(name).connectorStarted(name);
        }

        @Override
        public void connectorFailed(String name, String message, Throwable error) {
            resultFor(name).connectorFailed(name, message, error);
        }

        @Override
        public void connectorStopped(String name) {
            resultFor(name).connectorStopped(name);
        }

        @Override
        public void taskStarted(String name, int taskNumber, int totalTaskCount) {
            resultFor(name).taskStarted(name, taskNumber, totalTaskCount);
        }

        @Override
        public void taskStopped(String name, int taskNumber, int totalTaskCount) {
            resultFor(name).taskStopped(name, taskNumber, totalTaskCount);
        }

        public ConnectorResult forConnector(String connectorName) {
            return resultFor(connectorName);
        }

        protected final ConnectorResult resultFor(String connectorName) {
            return stats.computeIfAbsent(connectorName, this::createResultsFor);
        }
        
        protected ConnectorResult createResultsFor(String connectorName) {
            return new ConnectorResult(connectorName);
        }
        
        protected int count() {
            return stats.size();
        }

        @Override
        public String toString() {
            return stats.values()
                        .stream()
                        .map(ConnectorResult::toString)
                        .collect(Collectors.joining(System.lineSeparator()));
        }
    }

    
    private ConnectorCallbacks() {
    }

}

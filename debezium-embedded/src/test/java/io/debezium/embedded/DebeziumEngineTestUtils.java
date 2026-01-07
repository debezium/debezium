/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.Callback;

import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Collect;

/**
 * Utility classes and functions useful for testing implementations of {@link DebeziumEngine}.
 */
public class DebeziumEngineTestUtils {

    public static int appendLinesToSource(File inputFile, int numberOfLines, int initLineNumber) throws IOException {
        CharSequence[] lines = new CharSequence[numberOfLines];
        for (int i = 0; i != numberOfLines; ++i) {
            lines[i] = generateLine(initLineNumber + i + 1);
        }
        java.nio.file.Files.write(inputFile.toPath(), Collect.arrayListOf(lines), UTF_8, StandardOpenOption.APPEND);
        return numberOfLines;
    }

    public static int appendLinesToSource(File inputFile, String linePrefix, int numberOfLines, int initLineNumber) throws IOException {
        CharSequence[] lines = new CharSequence[numberOfLines];
        for (int i = 0; i != numberOfLines; ++i) {
            lines[i] = generateLine(linePrefix, initLineNumber + i + 1);
        }
        java.nio.file.Files.write(inputFile.toPath(), Collect.arrayListOf(lines), UTF_8, StandardOpenOption.APPEND);
        return numberOfLines;
    }

    public static String generateLine(int lineNumber) {
        return generateLine("Generated line number ", lineNumber);
    }

    public static String generateLine(String linePrefix, int lineNumber) {
        return linePrefix + lineNumber;
    }

    public static class FilterTransform implements Transformation<SourceRecord> {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            final String payload = (String) record.value();
            return payload.equals("Generated line number 1") || payload.equals("Generated line number 2") ? null
                    : record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void close() {
        }
    }

    public static class FilterPredicate implements Predicate<SourceRecord> {
        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public boolean test(SourceRecord sourceRecord) {
            return sourceRecord.value().equals("Generated line number 1");
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> map) {
        }
    }

    /**
     * A callback function to be notified when the connector completes.
     */
    public static class CompletionResult implements DebeziumEngine.CompletionCallback {
        private final DebeziumEngine.CompletionCallback delegate;
        private final CountDownLatch completed = new CountDownLatch(1);
        private boolean success;
        private String message;
        private Throwable error;

        public CompletionResult() {
            this(null);
        }

        public CompletionResult(DebeziumEngine.CompletionCallback delegate) {
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
}

class InterruptedConnector extends SimpleSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return InterruptedTask.class;
    }
}

class InterruptedTask extends SimpleSourceConnector.SimpleConnectorTask {

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        throw new InterruptedException();
    }
}

class NoOpConnector extends SimpleSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return NoOpTask.class;
    }
}

class NoOpTask extends SimpleSourceConnector.SimpleConnectorTask {

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return new ArrayList<SourceRecord>();
    }
}

class MultiTaskSimpleSourceConnector extends SimpleSourceConnector {

    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        config = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(config);
        }
        return configs;
    }
}

class RandomlyFailingDuringStartConnector extends MultiTaskSimpleSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return RandomlyFailingDuringStartTask.class;
    }
}

class RandomlyFailingDuringStartTask extends SimpleSourceConnector.SimpleConnectorTask {

    Random rand = new Random();

    @Override
    public void start(Map<String, String> props) {
        if (rand.nextBoolean()) {
            try {
                // Give other tasks chance to start
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("Unexpected interrupted exception");
            }
            throw new IllegalStateException("Exception during start of the task");
        }
    }
}

class InterruptingOffsetStore implements OffsetBackingStore {

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> collection) {
        // called by the offset reader. return null for no offsets stored.
        return new CompletableFuture<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit) {
                return new HashMap<ByteBuffer, ByteBuffer>();
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get() {
                return new HashMap<ByteBuffer, ByteBuffer>();
            }
        };
    }

    /**
     * Implementation that throws InterruptedException when offset commits are called.
     */
    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> map, Callback<Void> callback) {
        return new CompletableFuture<Void>() {
            @Override
            public Void get() throws InterruptedException {
                throw new InterruptedException();
            }

            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };
    }

    @Override
    public void configure(WorkerConfig workerConfig) {
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return null;
    }
}

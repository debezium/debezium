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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;

import io.debezium.connector.simple.SimpleSourceConnector;
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

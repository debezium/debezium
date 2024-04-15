/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.engine;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.config.Configuration;
import io.debezium.embedded.ConvertingEngineBuilderFactory;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.performance.engine.connector.PreComputedRecordsSourceConnector;
import io.debezium.util.IoUtil;

/**
 * JMH benchmark focused on speed of record processing of given {@link DebeziumEngine} implementation.
 */
public class DebeziumEnginePerf {

    @State(Scope.Thread)
    public static abstract class DebeziumEnginePerfTest {

        protected static final KeyValueChangeEventFormat KV_EVENT_FORMAT = KeyValueChangeEventFormat.of(Json.class, Json.class);
        protected static final String OFFSET_FILE_NAME = "offsets.txt";

        private DebeziumEngine<ChangeEvent<String, String>> engine;
        private ExecutorService executors;
        protected CountDownLatch finishLatch;

        @Param({ "100000", "1000000" })
        public int recordCount;

        public abstract DebeziumEngine createEngine();

        @Setup(Level.Iteration)
        public void doSetup() throws InterruptedException {
            delete(OFFSET_FILE_NAME);

            finishLatch = new CountDownLatch(recordCount);
            engine = createEngine();
            executors = Executors.newFixedThreadPool(1);
            executors.execute(engine);
        }

        @TearDown(Level.Iteration)
        public void doCleanup() throws IOException {
            try {
                if (engine != null) {
                    engine.close();
                }
                if (executors != null) {
                    executors.shutdown();
                    try {
                        executors.awaitTermination(60, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        executors.shutdownNow();
                    }
                }
            }
            finally {
                engine = null;
                executors = null;
            }
        }

        protected Consumer<ChangeEvent<String, String>> getRecordConsumer() {
            return record -> {
                if (record != null) {
                    finishLatch.countDown();
                }
            };
        }

        protected Path getPath(String relativePath) {
            return Paths.get(resolveDataDir(), relativePath).toAbsolutePath();
        }

        private void delete(String relativePath) {
            Path history = getPath(relativePath).toAbsolutePath();
            if (history != null) {
                history = history.toAbsolutePath();
                if (inTestDataDir(history)) {
                    try {
                        IoUtil.delete(history);
                    }
                    catch (IOException e) {
                        // ignored
                    }
                }
            }
        }

        private boolean inTestDataDir(Path path) {
            Path target = FileSystems.getDefault().getPath(resolveDataDir()).toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }

        private String resolveDataDir() {
            String value = System.getProperty("dbz.test.data.dir");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            value = System.getenv("DBZ_TEST_DATA_DIR");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            return "/tmp";
        }
    }

    @State(Scope.Thread)
    public static class AsyncEnginePerfTest extends DebeziumEnginePerfTest {
        @Param({ "1", "2", "4", "8", "16" })
        public int threadCount;

        @Param({ "ORDERED", "UNORDERED" })
        public String processingOrder;

        public DebeziumEngine createEngine() {
            Configuration config = Configuration.create()
                    .with(EmbeddedEngine.ENGINE_NAME, "async-engine")
                    .with(EmbeddedEngine.CONNECTOR_CLASS, PreComputedRecordsSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath(OFFSET_FILE_NAME).toAbsolutePath())
                    .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 3_600_000)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS, 100)
                    .with(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, 100)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_THREADS, threadCount)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_ORDER, processingOrder)
                    .build();

            return new ConvertingAsyncEngineBuilderFactory()
                    .builder(KV_EVENT_FORMAT)
                    .using(config.asProperties())
                    .notifying(getRecordConsumer())
                    .using(this.getClass().getClassLoader())
                    .build();
        }
    }

    @State(Scope.Thread)
    public static class EmbeddedEnginePerfTest extends DebeziumEnginePerfTest {

        public DebeziumEngine createEngine() {
            Configuration config = Configuration.create()
                    .with(EmbeddedEngine.ENGINE_NAME, "embedded-engine")
                    .with(EmbeddedEngine.CONNECTOR_CLASS, PreComputedRecordsSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath(OFFSET_FILE_NAME).toAbsolutePath())
                    .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 3_600_000)
                    .build();

            return new ConvertingEngineBuilderFactory()
                    .builder(KV_EVENT_FORMAT)
                    .using(config.asProperties())
                    .notifying(getRecordConsumer())
                    .using(this.getClass().getClassLoader())
                    .build();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1, time = 1)
    public void processRecordsAsyncEngine(AsyncEnginePerfTest test) throws InterruptedException {
        test.finishLatch.await();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1, time = 1)
    public void processRecordsEmbeddedEngine(EmbeddedEnginePerfTest test) throws InterruptedException {
        test.finishLatch.await();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.engine;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.performance.engine.connector.PreComputedRecordsSourceConnector;

/**
 * JMH benchmark focused on speed of record processing of given {@link DebeziumEngine} implementation not using any key/value converter.
 */
public class DebeziumEnginePerf {

    @State(Scope.Thread)
    public static class AsyncEnginePerfTest extends AbstractDebeziumEnginePerf {
        @Param({ "0", "1", "2", "4", "8", "16" })
        public int threadCount;

        @Param({ "ORDERED", "UNORDERED" })
        public String processingOrder;

        public DebeziumEngine createEngine() {
            Configuration.Builder confBuilder = Configuration.create()
                    .with(EmbeddedEngine.ENGINE_NAME, "async-engine")
                    .with(EmbeddedEngine.CONNECTOR_CLASS, PreComputedRecordsSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath(OFFSET_FILE_NAME).toAbsolutePath())
                    .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 3_600_000)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS, 100)
                    .with(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS, 100)
                    .with(AsyncEngineConfig.RECORD_PROCESSING_ORDER, processingOrder);
            // threadCount == 0 stands for the default configuration, when RECORD_PROCESSING_THREADS is not specified.
            if (threadCount > 0) {
                confBuilder.with(AsyncEngineConfig.RECORD_PROCESSING_THREADS, threadCount);
            }
            Configuration config = confBuilder.build();

            return new ConvertingAsyncEngineBuilderFactory()
                    .builder((KeyValueHeaderChangeEventFormat) null)
                    .using(config.asProperties())
                    .notifying(getRecordConsumer())
                    .using(this.getClass().getClassLoader())
                    .build();
        }
    }

    @State(Scope.Thread)
    public static class EmbeddedEnginePerfTest extends AbstractDebeziumEnginePerf {

        public DebeziumEngine createEngine() {
            Configuration config = Configuration.create()
                    .with(EmbeddedEngine.ENGINE_NAME, "embedded-engine")
                    .with(EmbeddedEngine.CONNECTOR_CLASS, PreComputedRecordsSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath(OFFSET_FILE_NAME).toAbsolutePath())
                    .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 3_600_000)
                    .build();

            return new EmbeddedEngine.EngineBuilder()
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

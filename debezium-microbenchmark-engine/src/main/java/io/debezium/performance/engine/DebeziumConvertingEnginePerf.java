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
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.performance.engine.connector.PreComputedRecordsSourceConnector;

/**
 * JMH benchmark focused on speed of record processing of given {@link DebeziumEngine} implementation using kay/value JSON converter.
 */
public class DebeziumConvertingEnginePerf {
    protected static final KeyValueChangeEventFormat KV_EVENT_FORMAT = KeyValueChangeEventFormat.of(Json.class, Json.class);

    @State(Scope.Thread)
    public static class AsyncEnginePerfTest extends AbstractDebeziumEnginePerf {
        @Param({ "1", "2", "4", "8", "16" })
        public int threadCount;

        @Param({ "ORDERED", "UNORDERED" })
        public String processingOrder;

        public DebeziumEngine createEngine() {
            Configuration config = Configuration.create()
                    .with(EmbeddedEngineConfig.ENGINE_NAME, "async-engine")
                    .with(EmbeddedEngineConfig.CONNECTOR_CLASS, PreComputedRecordsSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath(OFFSET_FILE_NAME).toAbsolutePath())
                    .with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, 3_600_000)
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

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1, time = 1)
    public void processRecordsAsyncEngine(AsyncEnginePerfTest test) throws InterruptedException {
        test.finishLatch.await();
    }
}

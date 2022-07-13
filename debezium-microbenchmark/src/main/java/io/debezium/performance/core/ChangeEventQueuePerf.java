/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.core;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE_IN_BYTES;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
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

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

public class ChangeEventQueuePerf {

    @Fork(1)
    @State(Scope.Thread)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 2, time = 5)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @BenchmarkMode({ Mode.Throughput })
    public static class ProducerPerf {

        private static final DataChangeEvent EVENT = new DataChangeEvent(new SourceRecord(Collections.emptyMap(),
                Collections.emptyMap(), "dummy", Schema.STRING_SCHEMA, "Change Data Capture Even via Debezium"));

        @Param({ "10", "50", "500" })
        private long pollIntervalMillis;

        private ChangeEventQueue<DataChangeEvent> changeEventQueue;
        private Thread consumer;

        @Setup(Level.Trial)
        public void setup() {
            changeEventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(Duration.ofMillis(pollIntervalMillis))
                    .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE).maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                    .maxQueueSizeInBytes(DEFAULT_MAX_QUEUE_SIZE_IN_BYTES).build();
            consumer = new Thread(() -> {
                try {
                    while (true) {
                        changeEventQueue.poll();
                    }
                }
                catch (InterruptedException ex) {
                    // exit thread
                }
            });
            consumer.start();
        }

        @Benchmark
        public void benchmarkProducer() throws InterruptedException {
            changeEventQueue.enqueue(EVENT);
        }

        @TearDown(Level.Trial)
        public void teardown() {
            consumer.interrupt();
        }

    }

    @Fork(1)
    @State(Scope.Thread)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 2, time = 5)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @BenchmarkMode({ Mode.Throughput })
    public static class ConsumerPerf {

        private static final DataChangeEvent EVENT = new DataChangeEvent(new SourceRecord(Collections.emptyMap(),
                Collections.emptyMap(), "dummy", Schema.STRING_SCHEMA, "Change Data Capture Even via Debezium"));;

        @Param({ "10", "50", "500" })
        private long pollIntervalMillis;

        private ChangeEventQueue<DataChangeEvent> changeEventQueue;
        private Thread producer;

        @Setup(Level.Trial)
        public void setup() {
            changeEventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(Duration.ofMillis(pollIntervalMillis))
                    .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE).maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                    .maxQueueSizeInBytes(DEFAULT_MAX_QUEUE_SIZE_IN_BYTES).build();
            producer = new Thread(() -> {
                try {
                    for (;;) {
                        changeEventQueue.enqueue(EVENT);
                    }
                }
                catch (InterruptedException ex) {
                    // exit thread
                }
            });
            producer.start();
        }

        @Benchmark
        public void benchmarkConsumer() throws InterruptedException {
            changeEventQueue.poll();
        }

        @TearDown(Level.Trial)
        public void teardown() {
            producer.interrupt();
        }

    }

    @Fork(1)
    @State(Scope.Thread)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({ Mode.AverageTime })
    public static class QueuePerf {

        private static final int TOTAL_RECORDS = 10_000_000;
        private static final DataChangeEvent EVENT = new DataChangeEvent(new SourceRecord(Collections.emptyMap(),
                Collections.emptyMap(), "dummy", Schema.STRING_SCHEMA, "Change Data Capture Even via Debezium"));;

        @Param({ "10", "50", "500" })
        long pollIntervalMillis;

        private ChangeEventQueue<DataChangeEvent> changeEventQueue;
        private Thread producer;
        private Thread consumer;

        @Setup(Level.Trial)
        public void setupInvocation() {
            changeEventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(Duration.ofMillis(pollIntervalMillis))
                    .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE).maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                    .maxQueueSizeInBytes(DEFAULT_MAX_QUEUE_SIZE_IN_BYTES).build();
        }

        @Setup(Level.Invocation)
        public void setup() {
            producer = new Thread(() -> {
                for (int i = 1; i <= TOTAL_RECORDS; i++) {
                    try {
                        changeEventQueue.enqueue(EVENT);
                    }
                    catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
            consumer = new Thread(new Runnable() {
                private long noOfRecords = 0;

                @Override
                public void run() {
                    while (noOfRecords < TOTAL_RECORDS) {
                        try {
                            noOfRecords += changeEventQueue.poll().size();
                        }
                        catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            });
        }

        @Benchmark
        public void benchmarkChangeEventQueue() throws InterruptedException {
            producer.start();
            consumer.start();
            producer.join();
            consumer.join();
        }

        @TearDown(Level.Invocation)
        public void teardown() {
            producer.interrupt();
            consumer.interrupt();
        }

    }

    @Fork(1)
    @State(Scope.Thread)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode({ Mode.AverageTime })
    public static class MultiWriterQueuePerf {

        private static final DataChangeEvent EVENT = new DataChangeEvent(new SourceRecord(Collections.emptyMap(),
                Collections.emptyMap(), "dummy", Schema.STRING_SCHEMA, "Change Data Capture Even via Debezium"));;
        private static final int TOTAL_RECORDS_PER_PRODUCER = 1_000_000;
        private static final int TOTAL_PRODUCERS = 10;

        @Param({ "10", "50", "500" })
        long pollIntervalMillis;

        private ChangeEventQueue<DataChangeEvent> changeEventQueue;
        private Thread[] producers;
        private Thread consumer;

        @Setup(Level.Trial)
        public void setupInvocation() {
            changeEventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(Duration.ofMillis(pollIntervalMillis))
                    .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE).maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                    .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                    .maxQueueSizeInBytes(DEFAULT_MAX_QUEUE_SIZE_IN_BYTES).build();
        }

        @Setup(Level.Invocation)
        public void setup() {
            producers = new Thread[TOTAL_PRODUCERS];
            for (int i = 0; i < TOTAL_PRODUCERS; i++) {
                producers[i] = new Thread(() -> {
                    try {
                        for (int j = 0; j < TOTAL_RECORDS_PER_PRODUCER; j++) {
                            changeEventQueue.enqueue(EVENT);
                        }
                    }
                    catch (InterruptedException ex) {
                        // exit thread
                    }
                });
            }

            long recordsToPoll = (long) TOTAL_RECORDS_PER_PRODUCER * TOTAL_PRODUCERS;
            consumer = new Thread(new Runnable() {
                private long noOfRecords = 0;

                @Override
                public void run() {
                    while (noOfRecords < recordsToPoll) {
                        try {
                            noOfRecords += changeEventQueue.poll().size();
                        }
                        catch (InterruptedException ex) {
                            // exit thread
                        }
                    }
                }
            });
        }

        @Benchmark
        public void benchmarkChangeEventQueue() throws InterruptedException {
            for (Thread producer : producers) {
                producer.start();
            }
            consumer.start();
            for (Thread producer : producers) {
                producer.join();
            }
            consumer.join();
        }

        @TearDown(Level.Invocation)
        public void teardown() {
            for (Thread producer : producers) {
                producer.interrupt();
            }
            consumer.interrupt();
        }

    }

}

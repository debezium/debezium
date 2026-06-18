/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.mysql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.pipeline.DataChangeEvent;

/**
 * Compares the SourceRecord extraction path used by MySqlConnectorTask#doPoll().
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class MySqlConnectorTaskPollPerf {

    @Param({ "1", "4", "16", "64", "256", "1024", "2048", "4096", "16384", "65536" })
    private int batchSize;

    private List<DataChangeEvent> records;

    @Setup(Level.Trial)
    public void setup() {
        records = createRecords(batchSize);
    }

    @Benchmark
    public List<SourceRecord> stream() {
        return streamConvert(records);
    }

    @Benchmark
    public List<SourceRecord> loop() {
        return loopConvert(records);
    }

    private static List<DataChangeEvent> createRecords(int batchSize) {
        final List<DataChangeEvent> events = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            events.add(new DataChangeEvent(new SourceRecord(Collections.emptyMap(), Collections.emptyMap(),
                    "dummy", Schema.STRING_SCHEMA, "Change Data Capture Event via Debezium")));
        }
        return List.copyOf(events);
    }

    private static List<SourceRecord> streamConvert(List<DataChangeEvent> records) {
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    private static List<SourceRecord> loopConvert(List<DataChangeEvent> records) {
        final List<SourceRecord> sourceRecords = new ArrayList<>(records.size());
        for (final DataChangeEvent record : records) {
            sourceRecords.add(record.getRecord());
        }
        return sourceRecords;
    }

    @State(Scope.Thread)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(1)
    @Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
    public static class Sustained {

        private static final int POLLS_PER_INVOCATION = 1_000;

        @Param({ "2048", "16384", "65536" })
        private int batchSize;

        private List<DataChangeEvent> records;

        @Setup(Level.Trial)
        public void setup() {
            records = createRecords(batchSize);
        }

        @Benchmark
        @OperationsPerInvocation(POLLS_PER_INVOCATION)
        public void stream(Blackhole blackhole) {
            for (int i = 0; i < POLLS_PER_INVOCATION; i++) {
                blackhole.consume(streamConvert(records));
            }
        }

        @Benchmark
        @OperationsPerInvocation(POLLS_PER_INVOCATION)
        public void loop(Blackhole blackhole) {
            for (int i = 0; i < POLLS_PER_INVOCATION; i++) {
                blackhole.consume(loopConvert(records));
            }
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.performance.Module;

/**
 * A basic test to calculate overhead of using SMTs.
 *
 * @author Jiri Pechanec <jpechane@redhat.com>
 *
 */
public class SmtOverheadPerf {

    private static class NewRecord implements Transformation<SourceRecord>, Versioned {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp(),
                    record.headers());
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public String version() {
            return Module.version();
        }
    }

    private static class NoOp implements Transformation<SourceRecord>, Versioned {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            return record;
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public String version() {
            return Module.version();
        }
    }

    @State(Scope.Thread)
    public static class TransformState {

        public Transformation<SourceRecord> newRecord;
        public Transformation<SourceRecord> noop;
        public SourceRecord delete;
        public SourceRecord create;

        @Setup(Level.Trial)
        public void doSetup() {
            final Schema schema = SchemaBuilder.struct().name("dummyenv").field("op", Schema.STRING_SCHEMA).build();

            final Struct deleteValue = new Struct(schema);
            deleteValue.put("op", "d");
            delete = new SourceRecord(new HashMap<>(), new HashMap<>(), "top1", 1, schema, delete);

            final Struct createValue = new Struct(schema);
            createValue.put("op", "c");
            create = new SourceRecord(new HashMap<>(), new HashMap<>(), "top1", 1, schema, create);

            newRecord = new NewRecord();
            newRecord.configure(new HashMap<>());

            noop = new NoOp();
            noop.configure(new HashMap<>());
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void newRecord(TransformState state) {
        state.newRecord.apply(state.create);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void noop(TransformState state) {
        state.noop.apply(state.create);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void base(TransformState state) {
    }
}

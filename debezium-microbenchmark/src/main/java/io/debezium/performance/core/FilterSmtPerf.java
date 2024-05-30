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

import io.debezium.data.Envelope;
import io.debezium.performance.Module;
import io.debezium.transforms.Filter;
import io.debezium.util.Collect;

/**
 * A basic test to compare performance of different implementations of filtering SMTs.
 *
 * @author Jiri Pechanec <jpechane@redhat.com>
 *
 */
public class FilterSmtPerf {

    private static class NativeFilter implements Transformation<SourceRecord>, Versioned {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public SourceRecord apply(SourceRecord record) {
            if (record.value() == null) {
                return record;
            }
            if (Envelope.Operation.DELETE.code().equals(((Struct) record.value()).getString(Envelope.FieldName.OPERATION))) {
                return null;
            }
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

        public Transformation<SourceRecord> nativeFilter;
        public Transformation<SourceRecord> groovyFilter;
        public Transformation<SourceRecord> jsFilter;
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

            nativeFilter = new NativeFilter();
            nativeFilter.configure(new HashMap<>());

            groovyFilter = new Filter<>();
            groovyFilter.configure(Collect.hashMapOf("language", "jsr223.groovy", "condition", "value.op == 'd'"));

            jsFilter = new Filter<>();
            jsFilter.configure(Collect.hashMapOf("language", "jsr223.graal.js", "condition", "value.get('op') == 'd'"));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void java(TransformState state) {
        state.nativeFilter.apply(state.create);
        state.nativeFilter.apply(state.create);
        state.nativeFilter.apply(state.delete);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void groovy(TransformState state) {
        state.groovyFilter.apply(state.create);
        state.groovyFilter.apply(state.create);
        state.groovyFilter.apply(state.delete);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void javascript(TransformState state) {
        state.jsFilter.apply(state.create);
        state.jsFilter.apply(state.create);
        state.jsFilter.apply(state.delete);
    }
}

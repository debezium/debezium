/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.core;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.data.SchemaUtil;

/**
 * Measures the per-event cost of rendering an event as a JSON-ish string, as done by the
 * pipeline metrics' last-event summary ({@code CommonEventMeter#onEvent} via
 * {@code EventFormatter} and {@link SchemaUtil#asDetailedString(Struct)}) for every
 * dispatched change record.
 */
@Fork(1)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode({ Mode.AverageTime })
public class EventSummaryPerf {

    @Param({ "4", "16" })
    private int stringFieldCount;

    private Struct value;

    @Setup
    public void setup() {
        final SchemaBuilder builder = SchemaBuilder.struct().name("bench.value");
        for (int i = 0; i < stringFieldCount; i++) {
            builder.field("col" + i, Schema.STRING_SCHEMA);
        }
        final Schema schema = builder.build();
        final Struct struct = new Struct(schema);
        for (int i = 0; i < stringFieldCount; i++) {
            struct.put("col" + i, "value-" + i + "-0123456789abcdef0123456789abcdef");
        }
        this.value = struct;
    }

    @Benchmark
    public String benchmarkAsDetailedStringStruct() {
        return SchemaUtil.asDetailedString(value);
    }
}

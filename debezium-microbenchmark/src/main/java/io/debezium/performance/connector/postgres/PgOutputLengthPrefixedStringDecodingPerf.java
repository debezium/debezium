/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class PgOutputLengthPrefixedStringDecodingPerf {

    private static final String CHARSET = "UTF-8";

    private final ByteBuffer buffer = ByteBuffer.wrap(new byte[3072], 32, 3000);

    private final Map<String, String> params = Map.of(
            "SMALL", "{}",
            "MEDIUM", "https://debezium.io/documentation/reference/3.6/connectors/postgresql.html",
            "LARGE", "B".repeat(2032) // Max before TOAST
    );

    @Param({ "SMALL", "MEDIUM", "LARGE" })
    private String size;

    @Setup(Level.Trial)
    public void setup() {
        this.buffer.clear();
        this.buffer.position(16);

        final var bytes = this.params.get(this.size).getBytes(StandardCharsets.UTF_8);
        this.buffer.putInt(bytes.length);
        this.buffer.put(bytes);
        this.buffer.position(16);
    }

    @Benchmark
    public void directByteArray(final Blackhole blackhole) {
        this.buffer.position(16);
        final var length = this.buffer.getInt();
        final var value = new String(this.buffer.array(),
                this.buffer.arrayOffset() + this.buffer.position(),
                length,
                StandardCharsets.UTF_8);
        this.buffer.position(this.buffer.position() + length);
        blackhole.consume(value);
    }

    @Benchmark
    public void arrayCopy(final Blackhole blackhole) {
        this.buffer.position(16);
        final var length = this.buffer.getInt();
        final var value = new byte[length];
        this.buffer.get(value, 0, length);
        blackhole.consume(new String(value, Charset.forName(CHARSET)));
    }
}

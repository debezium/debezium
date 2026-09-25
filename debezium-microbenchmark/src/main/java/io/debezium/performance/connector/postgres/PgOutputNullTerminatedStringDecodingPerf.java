/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
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
public class PgOutputNullTerminatedStringDecodingPerf {

    private final ByteBuffer buffer = ByteBuffer.wrap(new byte[512], 128, 256);

    private final Map<String, String> params = Map.of(
            "SMALL", "name",
            "MEDIUM", "last_modified_at",
            "LARGE", "long_object_name_of_the_default_postgres_limit_of_63_characters");

    @Param({ "SMALL", "MEDIUM", "LARGE" })
    private String size;

    @Setup(Level.Trial)
    public void setup() {
        this.buffer.clear();
        this.buffer.position(16);
        this.buffer.put(this.params.get(this.size).getBytes(StandardCharsets.UTF_8));
        this.buffer.put((byte) 0);
        this.buffer.position(16);
    }

    @Benchmark
    public void directByteArray(final Blackhole blackhole) {
        this.buffer.position(16);
        final var position = this.buffer.position();

        while (this.buffer.hasRemaining()) {
            if (this.buffer.get() == 0) {
                break;
            }
        }

        blackhole.consume(new String(this.buffer.array(),
                this.buffer.arrayOffset() + position,
                this.buffer.position() - position - 1,
                StandardCharsets.UTF_8));
    }

    @Benchmark
    public void byteArrayOutputStream(final Blackhole blackhole) {
        this.buffer.position(16);
        final var baos = new ByteArrayOutputStream();

        byte b;
        while ((b = this.buffer.get()) != 0) {
            baos.write(b);
        }

        blackhole.consume(baos.toString(StandardCharsets.UTF_8));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
// Since we are testing using same strings in each iteration, CompactStrings will reduce allocations greatly which is
// not realistic, as in real world, strings don't repeat that much.
@Fork(value = 1, jvmArgsAppend = { "-XX:-CompactStrings" })
@State(Scope.Benchmark)
abstract class BinaryVsTextDecodingPerfBase {

    protected String getString(final ByteBuffer buffer) {
        final var length = buffer.getInt();
        final var value = new String(buffer.array(),
                buffer.arrayOffset() + buffer.position(),
                length,
                StandardCharsets.UTF_8);
        buffer.position(buffer.position() + length);
        return value;
    }

    protected void putString(final ByteBuffer buffer, final String value) {
        final var bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }
}

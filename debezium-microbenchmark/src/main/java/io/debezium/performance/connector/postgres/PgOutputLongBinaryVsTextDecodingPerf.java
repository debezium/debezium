/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.nio.ByteBuffer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

public class PgOutputLongBinaryVsTextDecodingPerf extends BinaryVsTextDecodingPerfBase {

    private final ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[1024]);
    private final ByteBuffer textBuffer = ByteBuffer.wrap(new byte[1024]);

    @Setup(Level.Trial)
    public void setup() {
        this.binaryBuffer.putInt(8);
        this.binaryBuffer.putLong(9_192_631_770L);

        putString(this.textBuffer, "9192631770");
    }

    @Benchmark
    public void decodeBinary(final Blackhole blackhole) {
        this.binaryBuffer.position(0);

        blackhole.consume(this.binaryBuffer.getInt()); // ignore length
        blackhole.consume(this.binaryBuffer.getLong());
    }

    @Benchmark
    public void decodeText(final Blackhole blackhole) {
        this.textBuffer.position(0);

        final var text = getString(this.textBuffer);
        blackhole.consume(Long.parseLong(text));
    }
}
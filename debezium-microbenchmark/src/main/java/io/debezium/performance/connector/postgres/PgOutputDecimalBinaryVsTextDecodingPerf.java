/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.postgresql.util.ByteConverter;

public class PgOutputDecimalBinaryVsTextDecodingPerf extends BinaryVsTextDecodingPerfBase {

    private static final BigDecimal PI = new BigDecimal("3.141592653589793238");

    private final ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[1024]);
    private final ByteBuffer textBuffer = ByteBuffer.wrap(new byte[1024]);

    @Setup(Level.Trial)
    public void setup() {
        final var piBin = ByteConverter.numeric(PI);
        this.binaryBuffer.putInt(piBin.length);
        this.binaryBuffer.put(piBin);

        putString(this.textBuffer, PI.toString());
    }

    @Benchmark
    public void decodeBinary(final Blackhole blackhole) {
        this.binaryBuffer.position(0);

        final var length = this.binaryBuffer.getInt();
        final var position = this.binaryBuffer.position();
        this.binaryBuffer.position(this.binaryBuffer.position() + length);
        final var value = ByteConverter.numeric(this.binaryBuffer.array(),
                this.binaryBuffer.arrayOffset() + position, length);
        blackhole.consume(value);
    }

    @Benchmark
    public void decodeTextual(final Blackhole blackhole) {
        this.textBuffer.position(0);

        final var str = getString(this.textBuffer);
        final var decimal = new BigDecimal(str);
        blackhole.consume(decimal);
    }
}
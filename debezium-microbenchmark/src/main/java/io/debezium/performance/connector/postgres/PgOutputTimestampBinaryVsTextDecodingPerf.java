/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.connector.postgresql.connection.DateTimeFormat;

public class PgOutputTimestampBinaryVsTextDecodingPerf extends BinaryVsTextDecodingPerfBase {

    private static final Instant PG_EPOCH_INSTANT = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private final ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[1024]);
    private final ByteBuffer textBuffer = ByteBuffer.wrap(new byte[1024]);

    @Setup(Level.Trial)
    public void setup() {
        this.binaryBuffer.putInt(8);
        this.binaryBuffer.putLong(9_192_631_770L);

        putString(this.textBuffer, "2026-10-20 02:30:47");
    }

    @Benchmark
    public void decodeBinary(final Blackhole blackhole) {
        this.binaryBuffer.position(0);

        blackhole.consume(this.binaryBuffer.getInt()); // ignore length
        blackhole.consume(PG_EPOCH_INSTANT.plus(this.binaryBuffer.getLong(), ChronoUnit.MICROS));
    }

    @Benchmark
    public void decodeText(final Blackhole blackhole) {
        this.textBuffer.position(0);

        final var text = getString(this.textBuffer);
        blackhole.consume(DateTimeFormat.get().timestampToInstant(text));
    }
}

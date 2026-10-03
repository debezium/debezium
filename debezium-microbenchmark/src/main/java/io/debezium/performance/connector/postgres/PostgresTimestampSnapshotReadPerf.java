/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.postgresql.jdbc.TimestampUtils;

import io.debezium.connector.postgresql.connection.DateTimeFormat;

/**
 * Compares the two ways a snapshot can turn a text-format {@code timestamp} / {@code timestamptz}
 * column value into the {@code LocalDateTime} / UTC {@code OffsetDateTime} the converters consume.
 * <p>
 * The snapshot uses a plain {@code Statement}, so the driver receives every value as text. The
 * text path reads it with {@code rs.getString} and re-parses it with the connector's
 * {@link DateTimeFormat}, as {@code PostgresValueConverter} does. The JSR-310 path is what
 * {@code rs.getObject(i, LocalDateTime.class)} / {@code rs.getObject(i, OffsetDateTime.class)}
 * does with a text value: pgjdbc's {@link TimestampUtils} parses it, from a {@code String} for
 * {@code timestamp} and straight from the raw bytes for {@code timestamptz}.
 * <p>
 * Row fetch and result-set access are left out; they are the same for both paths. Each benchmark
 * parses every sample once, and the sample set covers fractional seconds, {@code BC} values and
 * non-UTC offsets. {@link #setup()} checks that both paths produce equal values for every sample,
 * so the benchmark never compares unequal work.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class PostgresTimestampSnapshotReadPerf {

    private static final String[] TIMESTAMP_SAMPLES = {
            "2024-03-15 10:30:59",
            "2024-03-15 10:30:59.123456",
            "1999-12-31 23:59:59.5",
            "1970-01-01 00:00:00",
            "1582-10-04 12:00:00",
            "0001-03-07 10:30:59 BC",
            "2100-06-30 08:15:00.000001",
            "2038-01-19 03:14:07.999"
    };

    private static final String[] TIMESTAMPTZ_SAMPLES = {
            "2024-03-15 10:30:59+00",
            "2024-03-15 10:30:59.123456+08",
            "1999-12-31 23:59:59.5-05",
            "1970-01-01 00:00:00+05:30",
            "1582-10-04 12:00:00+00",
            "0001-03-07 10:30:59+00 BC",
            "2100-06-30 08:15:00.000001+02",
            "2038-01-19 03:14:07.999-07"
    };

    private static final int SAMPLE_COUNT = 8;

    private byte[][] timestampBytes;
    private byte[][] timestamptzBytes;
    private TimestampUtils timestampUtils;

    @Setup(Level.Trial)
    public void setup() throws SQLException {
        timestampBytes = toBytes(TIMESTAMP_SAMPLES);
        timestamptzBytes = toBytes(TIMESTAMPTZ_SAMPLES);
        timestampUtils = new TimestampUtils(false, TimeZone::getDefault);

        for (byte[] value : timestampBytes) {
            assertEqual(timestampText(value), timestampJsr310(value), value);
        }
        for (byte[] value : timestamptzBytes) {
            assertEqual(timestamptzText(value), timestamptzJsr310(value), value);
        }
    }

    @Benchmark
    @OperationsPerInvocation(SAMPLE_COUNT)
    public void timestampText(Blackhole bh) {
        for (byte[] value : timestampBytes) {
            bh.consume(timestampText(value));
        }
    }

    @Benchmark
    @OperationsPerInvocation(SAMPLE_COUNT)
    public void timestampJsr310(Blackhole bh) throws SQLException {
        for (byte[] value : timestampBytes) {
            bh.consume(timestampJsr310(value));
        }
    }

    @Benchmark
    @OperationsPerInvocation(SAMPLE_COUNT)
    public void timestamptzText(Blackhole bh) {
        for (byte[] value : timestamptzBytes) {
            bh.consume(timestamptzText(value));
        }
    }

    @Benchmark
    @OperationsPerInvocation(SAMPLE_COUNT)
    public void timestamptzJsr310(Blackhole bh) throws SQLException {
        for (byte[] value : timestamptzBytes) {
            bh.consume(timestamptzJsr310(value));
        }
    }

    /** {@code rs.getString}, then {@code PostgresValueConverter#convertTimestampToLocalDateTime}. */
    private static LocalDateTime timestampText(byte[] value) {
        final String s = new String(value, StandardCharsets.UTF_8);
        return LocalDateTime.ofInstant(DateTimeFormat.get().timestampToInstant(s), ZoneOffset.UTC);
    }

    /** {@code rs.getObject(i, LocalDateTime.class)} on a text value. */
    private LocalDateTime timestampJsr310(byte[] value) throws SQLException {
        return timestampUtils.toLocalDateTime(new String(value, StandardCharsets.UTF_8));
    }

    /** {@code rs.getString}, then {@code PostgresValueConverter#convertTimestampWithZone}. */
    private static OffsetDateTime timestamptzText(byte[] value) {
        final String s = new String(value, StandardCharsets.UTF_8);
        return DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(s).withOffsetSameInstant(ZoneOffset.UTC);
    }

    /** {@code rs.getObject(i, OffsetDateTime.class)} on a text value; the driver normalizes it to UTC. */
    private OffsetDateTime timestamptzJsr310(byte[] value) throws SQLException {
        return timestampUtils.toOffsetDateTime(value).withOffsetSameInstant(ZoneOffset.UTC);
    }

    private static byte[][] toBytes(String[] samples) {
        if (samples.length != SAMPLE_COUNT) {
            throw new IllegalStateException("Expected " + SAMPLE_COUNT + " samples, got " + samples.length);
        }
        final byte[][] result = new byte[samples.length][];
        for (int i = 0; i < samples.length; i++) {
            result[i] = samples[i].getBytes(StandardCharsets.UTF_8);
        }
        return result;
    }

    private static void assertEqual(Object text, Object jsr310, byte[] value) {
        if (!text.equals(jsr310)) {
            throw new IllegalStateException("Paths disagree for '" + new String(value, StandardCharsets.UTF_8)
                    + "': text=" + text + ", jsr310=" + jsr310);
        }
    }
}

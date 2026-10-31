/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link RowDeserializers}, in particular the zero-date detection in the
 * {@code TIMESTAMP} deserializers. {@code DATE} / {@code DATETIME} already detected zero-date
 * via the {@code year/month/day == 0} branches; the {@code TIMESTAMP} branch was missing the
 * equivalent check (epoch-zero), causing streaming TIMESTAMP zero-dates to bypass the
 * configured zero-date fallback. See {@code zero.date.fallback.value*} on
 * {@link io.debezium.connector.binlog.BinlogConnectorConfig}.
 */
class RowDeserializersTest {

    /**
     * MySQL stores {@code TIMESTAMP '0000-00-00 00:00:00'} as 4 zero bytes on the wire. Because the
     * legal {@code TIMESTAMP} range starts at {@code 1970-01-01 00:00:01} UTC, a wire value of
     * epoch-zero unambiguously signals the zero-date sentinel and must be returned as {@code null}
     * so the downstream fallback can apply the configured per-type sentinel.
     */
    @Test
    void deserializeTimestampReturnsNullForZeroEpoch() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0 });
        assertThat(RowDeserializers.deserializeTimestamp(input)).isNull();
    }

    @Test
    void deserializeTimestampPreservesZeroEpochWhenRequested() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0 });

        Object result = RowDeserializers.deserializeTimestamp(input, true);

        assertThat(result).isInstanceOf(BinlogDateTimeValue.class);
        BinlogDateTimeValue value = (BinlogDateTimeValue) result;
        assertThat(value.getYear()).isZero();
        assertThat(value.getMonth()).isZero();
        assertThat(value.getDay()).isZero();
        assertThat(value.getHour()).isZero();
        assertThat(value.getMinute()).isZero();
        assertThat(value.getSecond()).isZero();
        assertThat(value.getNanos()).isZero();
    }

    /** Sanity check: a non-zero epoch second is converted to the corresponding UTC ZonedDateTime.
     *  V1 reads {@code epochSecond} as a little-endian 32-bit value (per shyiko binlog client). */
    @Test
    void deserializeTimestampReturnsZonedDateTimeForNonZeroEpoch() throws IOException {
        // 1234567890 (2009-02-13 23:31:30 UTC) = 0x499602D2 → little-endian bytes
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ (byte) 0xD2, 0x02, (byte) 0x96, 0x49 });

        Object result = RowDeserializers.deserializeTimestamp(input);

        assertThat(result).isInstanceOf(ZonedDateTime.class);
        ZonedDateTime zdt = (ZonedDateTime) result;
        assertThat(zdt.toEpochSecond()).isEqualTo(1234567890L);
        assertThat(zdt.getZone()).isEqualTo(ZoneOffset.UTC);
    }

    /** V2 (with fractional precision) at {@code meta=0} reads only 4 bytes; zero bytes signal zero-date. */
    @Test
    void deserializeTimestampV2ReturnsNullForZeroBytesNoFraction() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0 });
        assertThat(RowDeserializers.deserializeTimestampV2(0, input)).isNull();
    }

    @Test
    void deserializeTimestampV2PreservesZeroBytesWhenRequested() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0 });

        Object result = RowDeserializers.deserializeTimestampV2(0, input, true);

        assertThat(result).isInstanceOf(BinlogDateTimeValue.class);
        BinlogDateTimeValue value = (BinlogDateTimeValue) result;
        assertThat(value.getYear()).isZero();
        assertThat(value.getMonth()).isZero();
        assertThat(value.getDay()).isZero();
        assertThat(value.getNanos()).isZero();
    }

    /** V2 at {@code meta=0} with a non-zero epoch decodes normally. */
    @Test
    void deserializeTimestampV2ReturnsZonedDateTimeForNonZeroSeconds() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0x49, (byte) 0x96, 0x02, (byte) 0xD2 });

        Object result = RowDeserializers.deserializeTimestampV2(0, input);

        assertThat(result).isInstanceOf(ZonedDateTime.class);
        ZonedDateTime zdt = (ZonedDateTime) result;
        assertThat(zdt.toEpochSecond()).isEqualTo(1234567890L);
        assertThat(zdt.getZone()).isEqualTo(ZoneOffset.UTC);
    }

    /**
     * V2 at {@code meta=4} (microsecond precision, 2 fractional bytes) reads 4 + 2 = 6 bytes.
     * All-zero input → both {@code epochSecond == 0} and {@code nanoSeconds == 0} → null.
     */
    @Test
    void deserializeTimestampV2ReturnsNullForZeroBytesWithFraction() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0, 0, 0 });
        assertThat(RowDeserializers.deserializeTimestampV2(4, input)).isNull();
    }

    /**
     * Edge case: epoch-zero bytes with non-zero fractional bytes. MySQL semantics never produce this
     * (legal {@code TIMESTAMP(N)} starts at {@code 1970-01-01 00:00:01}), but the V2 zero-date guard
     * conservatively requires both seconds AND nanos to be zero. So this case decodes to a
     * non-null {@link ZonedDateTime} rather than being reclassified as a zero-date.
     */
    @Test
    void deserializeTimestampV2DoesNotReturnNullWhenOnlyFractionIsZero() throws IOException {
        // meta=4 (microsecond precision): bytes = {0,0,0,0} (epoch=0) + {0x10, 0x00} (fraction=4096)
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[]{ 0, 0, 0, 0, 0x10, 0x00 });

        Object result = RowDeserializers.deserializeTimestampV2(4, input);

        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(ZonedDateTime.class);
    }

    @Test
    @FixFor("debezium/dbz#2922")
    void deserializeDateReturnsDateWithYearZero() throws IOException {
        assertThat(RowDeserializers.deserializeDate(date(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, false))
                .isEqualTo(LocalDate.of(0, 1, 1));
    }

    @Test
    @FixFor("debezium/dbz#2922")
    void deserializeDatePreservesYearZeroWhenRequested() throws IOException {
        final Object result = RowDeserializers.deserializeDate(date(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, true);

        assertThat(result).isInstanceOf(BinlogDateValue.class);
        final BinlogDateValue value = (BinlogDateValue) result;
        assertThat(value.getYear()).isZero();
        assertThat(value.getMonth()).isEqualTo(1);
        assertThat(value.getDay()).isEqualTo(1);
    }

    @Test
    void deserializeDateReturnsNullForZeroMonth() throws IOException {
        assertThat(RowDeserializers.deserializeDate(date(2020, 0, 15), EventProcessingFailureHandlingMode.FAIL, false)).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2922")
    void deserializeDatetimeReturnsDatetimeWithYearZero() throws IOException {
        assertThat(RowDeserializers.deserializeDatetime(datetime(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, false))
                .isEqualTo(LocalDateTime.of(0, 1, 1, 0, 0));
        assertThat(RowDeserializers.deserializeDatetime(datetime(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, true))
                .isInstanceOf(BinlogDateTimeValue.class)
                .extracting(value -> ((BinlogDateTimeValue) value).getYear())
                .isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2922")
    void deserializeDatetimeV2ReturnsDatetimeWithYearZero() throws IOException {
        assertThat(RowDeserializers.deserializeDatetimeV2(0, datetimeV2(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, false))
                .isEqualTo(LocalDateTime.of(0, 1, 1, 0, 0));
        assertThat(RowDeserializers.deserializeDatetimeV2(0, datetimeV2(0, 1, 1), EventProcessingFailureHandlingMode.FAIL, true))
                .isInstanceOf(BinlogDateTimeValue.class)
                .extracting(value -> ((BinlogDateTimeValue) value).getYear())
                .isEqualTo(0);
    }

    /** {@code DATE}: 3 little-endian bytes of {@code (year * 16 + month) * 32 + day}. */
    private static ByteArrayInputStream date(int year, int month, int day) {
        final int value = (year * 16 + month) * 32 + day;
        return new ByteArrayInputStream(new byte[]{ (byte) value, (byte) (value >> 8), (byte) (value >> 16) });
    }

    /** {@code DATETIME} before MySQL 5.6.4: 8 little-endian bytes of {@code YYYYMMDDhhmmss}. */
    private static ByteArrayInputStream datetime(int year, int month, int day) {
        final long value = ((year * 100L + month) * 100L + day) * 1_000_000L;
        final byte[] bytes = new byte[8];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (value >> (8 * i));
        }
        return new ByteArrayInputStream(bytes);
    }

    /** {@code DATETIME2} without fractional seconds: 5 big-endian bytes of the sign bit, {@code year * 13 + month} and the day, at midnight. */
    private static ByteArrayInputStream datetimeV2(int year, int month, int day) {
        final long value = (1L << 39) | ((year * 13L + month) << 22) | ((long) day << 17);
        final byte[] bytes = new byte[5];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (value >> (8 * (bytes.length - 1 - i)));
        }
        return new ByteArrayInputStream(bytes);
    }
}

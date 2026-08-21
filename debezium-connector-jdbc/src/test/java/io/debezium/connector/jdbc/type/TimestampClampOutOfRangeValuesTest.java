/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.connect.ConnectTimestampType;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.connector.jdbc.type.debezium.MicroTimestampType;
import io.debezium.connector.jdbc.type.debezium.StructuredTimestampType;
import io.debezium.connector.jdbc.type.debezium.StructuredZonedTimestampType;
import io.debezium.doc.FixFor;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;
import io.debezium.time.ZonedTimestamp;

/**
 * Unit tests for the {@code timestamp.clamp.out.of.range.values} behavior of the temporal types,
 * verifying that values outside the dialect's representable timestamp range, such as BC-era
 * timestamps emitted by an Oracle source, are clamped to the dialect bounds when enabled.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
class TimestampClampOutOfRangeValuesTest {

    // Bounds modeled on the MySQL dialect's TIMESTAMP range
    private static final String MINIMUM_TIMESTAMP = "1970-01-01T00:00:01+00:00";
    private static final String MAXIMUM_TIMESTAMP = "2038-01-19T03:14:07+00:00";

    // 2018 BC (ISO proleptic year -2017), as emitted by the Oracle source for BC values
    private static final LocalDateTime BC_DATE_TIME = LocalDateTime.of(-2017, 3, 27, 12, 34, 56);
    private static final LocalDateTime ABOVE_MAXIMUM_DATE_TIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59);
    private static final LocalDateTime IN_RANGE_DATE_TIME = LocalDateTime.of(2018, 3, 27, 12, 34, 56);

    private static final LocalDateTime MINIMUM_DATE_TIME = LocalDateTime.of(1970, 1, 1, 0, 0, 1);
    private static final LocalDateTime MAXIMUM_DATE_TIME = LocalDateTime.of(2038, 1, 19, 3, 14, 7);

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range ZonedTimestamp values to the dialect bounds when enabled")
    void shouldClampOutOfRangeZonedTimestampValues() {
        final var type = new DebeziumZonedTimestampType();
        type.configure(config(true), dialect());

        final var below = type.bind(0, null, ZonedTimestamp.toIsoString(BC_DATE_TIME.atOffset(ZoneOffset.UTC), null));
        assertThat(below.get(0).getValue()).isEqualTo(MINIMUM_DATE_TIME.atOffset(ZoneOffset.UTC));

        final var above = type.bind(0, null, ZonedTimestamp.toIsoString(ABOVE_MAXIMUM_DATE_TIME.atOffset(ZoneOffset.UTC), null));
        assertThat(above.get(0).getValue()).isEqualTo(MAXIMUM_DATE_TIME.atOffset(ZoneOffset.UTC));

        final var inRange = type.bind(0, null, ZonedTimestamp.toIsoString(IN_RANGE_DATE_TIME.atOffset(ZoneOffset.UTC), null));
        assertThat(inRange.get(0).getValue()).isEqualTo(IN_RANGE_DATE_TIME.atOffset(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should pass out-of-range ZonedTimestamp values through unchanged when disabled")
    void shouldNotClampZonedTimestampValuesByDefault() {
        final var type = new DebeziumZonedTimestampType();
        type.configure(config(false), dialect());

        final var below = type.bind(0, null, ZonedTimestamp.toIsoString(BC_DATE_TIME.atOffset(ZoneOffset.UTC), null));
        assertThat(below.get(0).getValue()).isEqualTo(BC_DATE_TIME.atOffset(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range epoch-based timestamp values to the dialect bounds when enabled")
    void shouldClampOutOfRangeEpochTimestampValues() {
        final var type = new MicroTimestampType();
        type.configure(config(true), dialect());

        final var below = type.bind(0, null, toEpochMicros(BC_DATE_TIME));
        assertThat(below.get(0).getValue()).isEqualTo(MINIMUM_DATE_TIME);

        final var above = type.bind(0, null, toEpochMicros(ABOVE_MAXIMUM_DATE_TIME));
        assertThat(above.get(0).getValue()).isEqualTo(MAXIMUM_DATE_TIME);

        final var inRange = type.bind(0, null, toEpochMicros(IN_RANGE_DATE_TIME));
        assertThat(inRange.get(0).getValue()).isEqualTo(IN_RANGE_DATE_TIME);
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should pass out-of-range epoch-based timestamp values through unchanged when disabled")
    void shouldNotClampEpochTimestampValuesByDefault() {
        final var type = new MicroTimestampType();
        type.configure(config(false), dialect());

        final var below = type.bind(0, null, toEpochMicros(BC_DATE_TIME));
        assertThat(below.get(0).getValue()).isEqualTo(BC_DATE_TIME);
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range Kafka Connect timestamp values to the dialect bounds when enabled")
    void shouldClampOutOfRangeConnectTimestampValues() {
        final var type = new ConnectTimestampType();
        type.configure(config(true), dialect());

        final var below = type.bind(0, null, Date.from(BC_DATE_TIME.toInstant(ZoneOffset.UTC)));
        assertThat(below.get(0).getValue()).isEqualTo(MINIMUM_DATE_TIME);
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range structured timestamp values to the dialect bounds when enabled")
    void shouldClampOutOfRangeStructuredTimestampValues() {
        final var type = new StructuredTimestampType();
        type.configure(config(true), dialect());

        final var below = type.bind(0, null, StructuredTimestamp.from(BC_DATE_TIME));
        assertThat(below.get(0).getValue()).isEqualTo(MINIMUM_DATE_TIME);
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range structured zoned timestamp values to the dialect bounds when enabled")
    void shouldClampOutOfRangeStructuredZonedTimestampValues() {
        final var type = new StructuredZonedTimestampType();
        type.configure(config(true), dialect());

        final var below = type.bind(0, null, StructuredZonedTimestamp.from(BC_DATE_TIME.atOffset(ZoneOffset.UTC)));
        assertThat(below.get(0).getValue()).isEqualTo(MINIMUM_DATE_TIME.atOffset(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should clamp out-of-range ZonedTimestamp values in dialects that override the normal value binding")
    void shouldClampOutOfRangeZonedTimestampValuesInDialectOverrides() {
        final var bcValue = ZonedTimestamp.toIsoString(BC_DATE_TIME.atOffset(ZoneOffset.UTC), null);

        final var db2Type = new io.debezium.connector.jdbc.dialect.db2.ZonedTimestampType();
        db2Type.configure(config(true), dialect());
        assertThat(db2Type.bind(0, null, bcValue).get(0).getValue())
                .isEqualTo(java.sql.Timestamp.valueOf(MINIMUM_DATE_TIME));

        final var db2iType = new io.debezium.connector.jdbc.dialect.db2i.ZonedTimestampType();
        db2iType.configure(config(true), dialect());
        assertThat(db2iType.bind(0, null, bcValue).get(0).getValue())
                .isEqualTo(java.sql.Timestamp.valueOf(MINIMUM_DATE_TIME));

        final var oracleType = new io.debezium.connector.jdbc.dialect.oracle.ZonedTimestampType();
        oracleType.configure(config(true), dialect());
        assertThat(((java.time.ZonedDateTime) oracleType.bind(0, null, bcValue).get(0).getValue()).toInstant())
                .isEqualTo(MINIMUM_DATE_TIME.toInstant(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should not clamp when the dialect has no finite timestamp bounds")
    void shouldNotClampWhenDialectBoundsAreNotFiniteTimestamps() {
        // PostgreSQL represents infinity with markers the database understands natively
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getTimestampNegativeInfinityValue()).thenReturn("-infinity");
        when(dialect.getTimestampPositiveInfinityValue()).thenReturn("infinity");

        final var type = new DebeziumZonedTimestampType();
        type.configure(config(true), dialect);

        final var below = type.bind(0, null, ZonedTimestamp.toIsoString(BC_DATE_TIME.atOffset(ZoneOffset.UTC), null));
        assertThat(below.get(0).getValue()).isEqualTo(BC_DATE_TIME.atOffset(ZoneOffset.UTC));
    }

    @Test
    @FixFor("debezium/dbz#1008")
    @DisplayName("Should default timestamp.clamp.out.of.range.values to disabled")
    void shouldDefaultToDisabled() {
        final var config = new JdbcSinkConnectorConfig(Map.of());
        assertThat(config.isTimestampClampForOutOfRangeValuesEnabled()).isFalse();

        final var enabled = new JdbcSinkConnectorConfig(Map.of(JdbcSinkConnectorConfig.TIMESTAMP_CLAMP_OUT_OF_RANGE_VALUES, "true"));
        assertThat(enabled.isTimestampClampForOutOfRangeValuesEnabled()).isTrue();
    }

    private static long toEpochMicros(LocalDateTime dateTime) {
        final var instant = dateTime.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }

    private static JdbcSinkConnectorConfig config(boolean clampEnabled) {
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.isTimestampClampForOutOfRangeValuesEnabled()).thenReturn(clampEnabled);
        return config;
    }

    private static DatabaseDialect dialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getTimestampNegativeInfinityValue()).thenReturn(MINIMUM_TIMESTAMP);
        when(dialect.getTimestampPositiveInfinityValue()).thenReturn(MAXIMUM_TIMESTAMP);
        when(dialect.isTimeZoneSet()).thenReturn(false);
        return dialect;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.engine.jdbc.Size;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities.ZonedTimestampSupport;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

@Tag("UnitTests")
class StructuredTemporalPrecisionTest {

    @Test
    @DisplayName("Should keep default precision for non-structured timestamp zero precision")
    void shouldKeepDefaultPrecisionForNonStructuredTimestampZeroPrecision() {
        final var type = new io.debezium.connector.jdbc.type.connect.ConnectTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = org.apache.kafka.connect.data.Timestamp.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(6)");
    }

    @Test
    @DisplayName("Should keep default precision for non-structured time zero precision")
    void shouldKeepDefaultPrecisionForNonStructuredTimeZeroPrecision() {
        final var type = new io.debezium.connector.jdbc.type.connect.ConnectTimeType();
        type.configure(config(), timeDialect());
        final var schema = org.apache.kafka.connect.data.Time.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(6)");
    }

    @Test
    @DisplayName("Should use propagated source column scale for structured timestamp precision")
    void shouldUseSourceColumnScaleForStructuredTimestampPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "6")
                .parameter("__debezium.source.column.length", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(6)");
    }

    @Test
    @DisplayName("Should prefer structured precision metadata over propagated source metadata")
    void shouldPreferStructuredPrecisionMetadata() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder(7)
                .parameter("__debezium.source.column.scale", "3")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(7)");
    }

    @Test
    @DisplayName("Should preserve source column zero precision")
    void shouldPreserveSourceColumnZeroPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "0")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(0)");
    }

    @Test
    @DisplayName("Should fallback to propagated source column precision")
    void shouldFallbackToSourceColumnPrecision() {
        final var type = new StructuredTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.scale", "4")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamp(4)");
    }

    @Test
    @DisplayName("Should use propagated source column scale for structured time precision")
    void shouldUseSourceColumnScaleForStructuredTimePrecision() {
        final var type = new StructuredTimeType();
        type.configure(config(), timeDialect());
        final var schema = StructuredTime.builder()
                .parameter("__debezium.source.column.scale", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(6)");
    }

    @Test
    @DisplayName("Should use propagated source column scale for structured zoned timestamp precision")
    void shouldUseSourceColumnScaleForStructuredZonedTimestampPrecision() {
        final var type = new StructuredZonedTimestampType();
        type.configure(config(), timestampDialect());
        final var schema = StructuredZonedTimestamp.builder()
                .parameter("__debezium.source.column.scale", "3")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("timestamptz(3)");
    }

    @Test
    @DisplayName("Should fail only when discarded timestamp digits are non-zero")
    void shouldFailOnlyForNonZeroDiscardedDigits() {
        final var capabilities = TargetTemporalCapabilities.defaults(9, 9);
        final var type = configuredTimestampType(TemporalPrecisionLossHandlingMode.FAIL, capabilities);
        final var schema = StructuredTimestamp.builder(9).build();
        final var column = timestampColumn(6);
        final var exactValue = StructuredTimestamp.from(schema, 2026, 7, 17, 12, 13, 14, 123_456_000, 9);
        final var lossyValue = StructuredTimestamp.from(schema, 2026, 7, 17, 12, 13, 14, 123_456_789, 9);

        assertThatCode(() -> type.validate(column, schema, exactValue)).doesNotThrowAnyException();
        assertThatThrownBy(() -> type.validate(column, schema, lossyValue))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("target column 'ts'")
                .hasMessageContaining("precision 6");
    }

    @Test
    @DisplayName("Should explicitly truncate and round timestamp fractions")
    void shouldApplyConfiguredPrecisionReduction() {
        final var capabilities = TargetTemporalCapabilities.defaults(9, 9);
        final var schema = StructuredTimestamp.builder(9).build();
        final var column = timestampColumn(6);
        final var value = StructuredTimestamp.from(schema, 2026, 7, 17, 12, 13, 14, 123_456_789, 9);

        final var truncateType = configuredTimestampType(TemporalPrecisionLossHandlingMode.TRUNCATE, capabilities);
        final var roundType = configuredTimestampType(TemporalPrecisionLossHandlingMode.ROUND, capabilities);

        assertThat(truncateType.bind(1, column, schema, value).get(0).getValue())
                .isEqualTo(LocalDateTime.of(2026, 7, 17, 12, 13, 14, 123_456_000));
        assertThat(roundType.bind(1, column, schema, value).get(0).getValue())
                .isEqualTo(LocalDateTime.of(2026, 7, 17, 12, 13, 14, 123_457_000));
    }

    @Test
    @DisplayName("Should explicitly reduce picoseconds for nanosecond targets")
    void shouldReducePicosecondsForNanosecondTargets() {
        final var capabilities = TargetTemporalCapabilities.defaults(9, 9);
        final var schema = StructuredTimestamp.builder(12).build();
        final var column = timestampColumn(9);
        final var value = StructuredTimestamp.fromPicoseconds(schema, 2026, 7, 17, 12, 13, 14, 123_456_789_600L, 12);

        final var failType = configuredTimestampType(TemporalPrecisionLossHandlingMode.FAIL, capabilities);
        final var truncateType = configuredTimestampType(TemporalPrecisionLossHandlingMode.TRUNCATE, capabilities);
        final var roundType = configuredTimestampType(TemporalPrecisionLossHandlingMode.ROUND, capabilities);

        assertThatThrownBy(() -> failType.bind(1, column, schema, value))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("precision 9");
        assertThat(truncateType.bind(1, column, schema, value).get(0).getValue())
                .isEqualTo(LocalDateTime.of(2026, 7, 17, 12, 13, 14, 123_456_789));
        assertThat(roundType.bind(1, column, schema, value).get(0).getValue())
                .isEqualTo(LocalDateTime.of(2026, 7, 17, 12, 13, 14, 123_456_790));
    }

    @Test
    @DisplayName("Should carry a rounded timestamp fraction into the next second")
    void shouldCarryRoundedFraction() {
        final var type = configuredTimestampType(
                TemporalPrecisionLossHandlingMode.ROUND, TargetTemporalCapabilities.defaults(9, 9));
        final var schema = StructuredTimestamp.builder(9).build();
        final var value = StructuredTimestamp.from(schema, 2026, 7, 17, 12, 13, 14, 999_999_999, 9);

        assertThat(type.bind(1, timestampColumn(6), schema, value).get(0).getValue())
                .isEqualTo(LocalDateTime.of(2026, 7, 17, 12, 13, 15));
    }

    @Test
    @DisplayName("Should reject zoned timestamp metadata that the target cannot preserve")
    void shouldRejectUnsupportedZonedTimestampMetadata() {
        final var schema = StructuredZonedTimestamp.builder(9).build();
        final var offsetValue = StructuredZonedTimestamp.from(
                schema, OffsetDateTime.of(2026, 7, 17, 12, 13, 14, 0, ZoneOffset.ofHours(9)), null, 9);
        final var regionValue = StructuredZonedTimestamp.from(
                schema, OffsetDateTime.of(2026, 7, 17, 12, 13, 14, 0, ZoneOffset.ofHours(9)), "Asia/Seoul", 9);
        final var fixedUtcValue = StructuredZonedTimestamp.from(
                schema, OffsetDateTime.of(2026, 7, 17, 12, 13, 14, 0, ZoneOffset.UTC), "Z", 9);

        final var noZoneType = configuredZonedTimestampType(TargetTemporalCapabilities.defaults(9, 9));
        assertThatCode(() -> noZoneType.validate(timestampColumn(9), schema, fixedUtcValue)).doesNotThrowAnyException();
        assertThatThrownBy(() -> noZoneType.validate(timestampColumn(9), schema, offsetValue))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("offset 32400");

        final var offsetOnlyType = configuredZonedTimestampType(
                TargetTemporalCapabilities.defaults(9, 9).withZonedTimestampSupport(ZonedTimestampSupport.OFFSET));
        assertThatThrownBy(() -> offsetOnlyType.validate(timestampColumn(9), schema, regionValue))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("region 'Asia/Seoul'");
    }

    @Test
    @DisplayName("Should use dialect precision when JDBC metadata is unreliable")
    void shouldUseDialectPrecisionForUnreliableMetadata() {
        final var capabilities = TargetTemporalCapabilities.defaults(6, 6)
                .withTimestampColumnPrecisionReliable(false);

        assertThat(capabilities.targetTimestampPrecision(timestampColumn(0))).isEqualTo(6);
    }

    private SinkConnectorConfig config() {
        final SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        return config;
    }

    private StructuredTimestampType configuredTimestampType(TemporalPrecisionLossHandlingMode mode, TargetTemporalCapabilities capabilities) {
        final var type = new StructuredTimestampType();
        type.configure(jdbcConfig(mode), temporalDialect(capabilities));
        return type;
    }

    private StructuredZonedTimestampType configuredZonedTimestampType(TargetTemporalCapabilities capabilities) {
        final var type = new StructuredZonedTimestampType();
        type.configure(jdbcConfig(TemporalPrecisionLossHandlingMode.FAIL), temporalDialect(capabilities));
        return type;
    }

    private JdbcSinkConnectorConfig jdbcConfig(TemporalPrecisionLossHandlingMode mode) {
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(mode);
        return config;
    }

    private DatabaseDialect temporalDialect(TargetTemporalCapabilities capabilities) {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(capabilities);
        return dialect;
    }

    private ColumnDescriptor timestampColumn(int precision) {
        return ColumnDescriptor.builder()
                .columnName("ts")
                .jdbcType(Types.TIMESTAMP)
                .typeName("timestamp")
                .scale(precision)
                .build();
    }

    private DatabaseDialect timestampDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getMaxTimestampPrecision()).thenReturn(9);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getJdbcTypeName(eq(Types.TIMESTAMP), any(Size.class)))
                .thenAnswer(invocation -> "timestamp(" + size(invocation.getArgument(1)) + ")");
        when(dialect.getJdbcTypeName(eq(Types.TIMESTAMP_WITH_TIMEZONE), any(Size.class)))
                .thenAnswer(invocation -> "timestamptz(" + size(invocation.getArgument(1)) + ")");
        return dialect;
    }

    private DatabaseDialect timeDialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getMaxTimePrecision()).thenReturn(9);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getDefaultTimePrecision()).thenReturn(6);
        when(dialect.getJdbcTypeName(eq(Types.TIME), any(Size.class)))
                .thenAnswer(invocation -> "time(" + size(invocation.getArgument(1)) + ")");
        return dialect;
    }

    private Integer size(Size size) {
        return size.getPrecision();
    }
}

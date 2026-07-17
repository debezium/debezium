/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.engine.jdbc.Size;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind zero date components as MySQL date literal")
    void shouldBindZeroDateComponents() {
        final var schema = StructuredDate.schema();
        final var value = StructuredDate.from(schema, 0, 0, 0);

        final var bindings = StructuredDateType.INSTANCE.bind(1, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("0000-00-00");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(StructuredDateType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("'0000-00-00'");
    }

    @Test
    @DisplayName("Should bind invalid date components as MySQL timestamp literal")
    void shouldBindInvalidTimestampComponents() {
        final var schema = StructuredTimestamp.schema();
        final var value = StructuredTimestamp.from(schema, 2026, 2, 31, 12, 13, 14, 123_456_000);
        final var type = configuredTimestampType(TemporalPrecisionLossHandlingMode.FAIL);

        final var bindings = type.bind(2, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("2026-02-31 12:13:14.123456");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(type.getDefaultValueBinding(schema, value)).isEqualTo("'2026-02-31 12:13:14.123456'");
    }

    @Test
    @DisplayName("Should bind structured duration as MySQL time literal")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 0, 0, 0, -838, -59, -58, -999_999_000);
        final var type = configuredDurationType(TemporalPrecisionLossHandlingMode.FAIL);

        final var bindings = type.bind(3, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("-838:59:58.999999");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(type.getDefaultValueBinding(schema, value)).isEqualTo("'-838:59:58.999999'");
    }

    @Test
    @DisplayName("Should use propagated source column length for structured duration precision")
    void shouldUseSourceColumnLengthForStructuredDurationPrecision() {
        final var type = new StructuredDurationType();
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getJdbcTypeName(eq(Types.TIME), any(Size.class)))
                .thenAnswer(invocation -> "time(" + ((Size) invocation.getArgument(1)).getPrecision() + ")");
        when(dialect.getTargetTemporalCapabilities()).thenReturn(mysqlCapabilities());
        final SinkConnectorConfig config = mock(SinkConnectorConfig.class);
        when(config.useTimeZone()).thenReturn("UTC");
        type.configure(config, dialect);

        final var schema = StructuredDuration.builder()
                .parameter("__debezium.source.column.length", "6")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("time(6)");
    }

    @Test
    @DisplayName("Should reject calendar duration kinds for MySQL time")
    void shouldRejectCalendarDurationKinds() {
        final var type = configuredDurationType(TemporalPrecisionLossHandlingMode.FAIL);
        final var yearMonthSchema = StructuredDuration.builder(0, StructuredDuration.Kind.YEAR_MONTH).build();

        assertThatThrownBy(() -> type.getTypeName(yearMonthSchema, false))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("year-month")
                .hasMessageContaining("MySQL TIME");

        final var legacySchema = StructuredDuration.schema();
        final var valueWithDays = StructuredDuration.from(legacySchema, 0, 0, 1, 2, 3, 4, 0);
        assertThatThrownBy(() -> type.validate(timeColumn(6), legacySchema, valueWithDays))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("day-time");

        final var elapsedSchema = StructuredDuration.builder(6, StructuredDuration.Kind.ELAPSED_TIME).build();
        final var inconsistentValue = StructuredDuration.from(elapsedSchema, 0, 0, 1, 2, 3, 4, 0, 6);
        assertThatThrownBy(() -> type.validate(timeColumn(6), elapsedSchema, inconsistentValue))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("do not match schema kind 'elapsed-time'");
    }

    @Test
    @DisplayName("Should explicitly truncate and round MySQL duration fractions")
    void shouldReduceMySqlDurationPrecision() {
        final var schema = StructuredDuration.builder(9, StructuredDuration.Kind.ELAPSED_TIME).build();
        final var value = StructuredDuration.from(schema, 0, 0, 0, 1, 2, 3, 123_456_789, 9);

        final var truncateType = configuredDurationType(TemporalPrecisionLossHandlingMode.TRUNCATE);
        final var roundType = configuredDurationType(TemporalPrecisionLossHandlingMode.ROUND);

        assertThat(truncateType.bind(1, timeColumn(6), schema, value).get(0).getValue())
                .isEqualTo("001:02:03.123456");
        assertThat(roundType.bind(1, timeColumn(6), schema, value).get(0).getValue())
                .isEqualTo("001:02:03.123457");
    }

    @Test
    @DisplayName("Should apply round mode to MySQL structured temporal defaults")
    void shouldRoundMySqlStructuredTemporalDefaults() {
        final var timestampSchema = StructuredTimestamp.builder(9).build();
        final var timestamp = StructuredTimestamp.from(
                timestampSchema, 2026, 7, 17, 12, 13, 14, 123_456_789, 9);
        final var durationSchema = StructuredDuration.builder(9, StructuredDuration.Kind.ELAPSED_TIME).build();
        final var duration = StructuredDuration.from(durationSchema, 0, 0, 0, 1, 2, 3, 123_456_789, 9);

        assertThat(configuredTimestampType(TemporalPrecisionLossHandlingMode.ROUND)
                .getDefaultValueBinding(timestampSchema, timestamp))
                .isEqualTo("'2026-07-17 12:13:14.123457'");
        assertThat(configuredDurationType(TemporalPrecisionLossHandlingMode.ROUND)
                .getDefaultValueBinding(durationSchema, duration))
                .isEqualTo("'001:02:03.123457'");
        assertThatThrownBy(() -> configuredDurationType(TemporalPrecisionLossHandlingMode.FAIL)
                .getDefaultValueBinding(durationSchema, duration))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("precision 6");
    }

    private StructuredTimestampType configuredTimestampType(TemporalPrecisionLossHandlingMode mode) {
        final var type = new StructuredTimestampType();
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(mode);
        when(dialect.getMaxTimestampPrecision()).thenReturn(6);
        when(dialect.getDefaultTimestampPrecision()).thenReturn(6);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(mysqlCapabilities());
        type.configure(config, dialect);
        return type;
    }

    private StructuredDurationType configuredDurationType(TemporalPrecisionLossHandlingMode mode) {
        final var type = new StructuredDurationType();
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(mode);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(mysqlCapabilities());
        type.configure(config, dialect);
        return type;
    }

    private TargetTemporalCapabilities mysqlCapabilities() {
        return TargetTemporalCapabilities.defaults(6, 6)
                .withDurationKinds(java.util.EnumSet.of(StructuredDuration.Kind.ELAPSED_TIME));
    }

    private ColumnDescriptor timeColumn(int precision) {
        return ColumnDescriptor.builder()
                .columnName("duration")
                .jdbcType(Types.TIME)
                .typeName("time")
                .scale(precision)
                .build();
    }
}

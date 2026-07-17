/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredTimestampType;
import io.debezium.connector.jdbc.type.debezium.StructuredZonedTimestampType;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTime;
import io.debezium.time.StructuredZonedTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind structured time as Oracle timestamp value")
    void shouldBindStructuredTimeAsTimestamp() {
        final var schema = StructuredTime.schema();
        final var value = StructuredTime.from(schema, LocalTime.of(12, 13, 14, 123_456_789));

        final var bindings = StructuredTimeType.INSTANCE.bind(1, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo(LocalDateTime.of(LocalDate.EPOCH, LocalTime.of(12, 13, 14, 123_456_789)));
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.TIMESTAMP);
    }

    @Test
    @DisplayName("Should preserve Oracle binding shape after precision reduction")
    void shouldReduceStructuredTimeAsTimestampValue() {
        final var schema = StructuredTime.builder(9).build();
        final var value = StructuredTime.from(schema, LocalTime.of(12, 13, 14, 123_456_789), 9);
        final var type = new io.debezium.connector.jdbc.dialect.oracle.StructuredTimeType();
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(config.useTimeZone()).thenReturn("UTC");
        when(config.getTemporalPrecisionLossHandlingMode()).thenReturn(TemporalPrecisionLossHandlingMode.TRUNCATE);
        when(dialect.getTargetTemporalCapabilities()).thenReturn(TargetTemporalCapabilities.defaults(9, 9));
        type.configure(config, dialect);

        final var binding = type.bind(1, timeColumn(3), schema, value).get(0);

        assertThat(binding.getValue())
                .isEqualTo(LocalDateTime.of(LocalDate.EPOCH, LocalTime.of(12, 13, 14, 123_000_000)));
        assertThat(binding.getTargetSqlType()).isEqualTo(Types.TIMESTAMP);
    }

    @Test
    @DisplayName("Should bind structured zoned time as Oracle time-with-zone value")
    void shouldBindStructuredZonedTime() {
        final var schema = StructuredZonedTime.schema();
        final var value = StructuredZonedTime.from(schema, OffsetTime.of(12, 13, 14, 123_456_789, ZoneOffset.ofHours(9)));

        final var bindings = StructuredZonedTimeType.INSTANCE.bind(2, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isInstanceOf(ZonedDateTime.class);
        assertThat(((ZonedDateTime) bindings.get(0).getValue()).toLocalDate()).isEqualTo(LocalDate.EPOCH);
        assertThat(((ZonedDateTime) bindings.get(0).getValue()).toOffsetDateTime().toOffsetTime())
                .isEqualTo(OffsetTime.of(12, 13, 14, 123_456_789, ZoneOffset.ofHours(9)));
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.TIME_WITH_TIMEZONE);
    }

    @Test
    @DisplayName("Should bind structured year-month duration as Oracle interval")
    void shouldBindStructuredYearMonthDuration() {
        final var schema = StructuredDuration.builder()
                .parameter("__debezium.source.column.type", "INTERVAL YEAR TO MONTH")
                .build();
        final var value = StructuredDuration.from(schema, -3, -6, 0, 0, 0, 0, 0);

        final var bindings = StructuredDurationType.INSTANCE.bind(3, schema, value);

        assertThat(StructuredDurationType.INSTANCE.getTypeName(schema, false)).isEqualTo("INTERVAL YEAR TO MONTH");
        assertThat(StructuredDurationType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("TO_YMINTERVAL(?)");
        assertThat(StructuredDurationType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("TO_YMINTERVAL('-3-06')");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("-3-06");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Should bind structured day-second duration as Oracle interval")
    void shouldBindStructuredDaySecondDuration() {
        final var schema = StructuredDuration.builder()
                .parameter("__debezium.source.column.type", "INTERVAL DAY TO SECOND")
                .parameter("__debezium.source.column.length", "3")
                .parameter("__debezium.source.column.scale", "9")
                .build();
        final var value = StructuredDuration.from(schema, 0, 0, -1, -2, -3, -4, -567_890_000);

        final var bindings = StructuredDurationType.INSTANCE.bind(4, schema, value);

        assertThat(StructuredDurationType.INSTANCE.getTypeName(schema, false)).isEqualTo("INTERVAL DAY(3) TO SECOND(9)");
        assertThat(StructuredDurationType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("TO_DSINTERVAL(?)");
        assertThat(StructuredDurationType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("TO_DSINTERVAL('-1 02:03:04.567890000')");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("-1 02:03:04.567890000");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Should map structured elapsed duration to Oracle day-second interval")
    void shouldBindStructuredElapsedDuration() {
        final var schema = StructuredDuration.builder(6, StructuredDuration.Kind.ELAPSED_TIME).build();
        final var value = StructuredDuration.from(schema, 0, 0, 0, 34, 5, 6, 789_000_000, 6);

        assertThat(StructuredDurationType.INSTANCE.getTypeName(schema, false))
                .isEqualTo("INTERVAL DAY TO SECOND(6)");
        assertThat(StructuredDurationType.INSTANCE.getQueryBinding(null, schema, value))
                .isEqualTo("TO_DSINTERVAL(?)");
        assertThat(StructuredDurationType.INSTANCE.bind(1, schema, value).get(0).getValue())
                .isEqualTo("1 10:05:06.789000");
    }

    @Test
    @DisplayName("Should fail structured timestamp infinity for Oracle")
    void shouldFailStructuredTimestampInfinity() {
        final var schema = StructuredTimestamp.schema();

        assertThatThrownBy(() -> StructuredTimestampType.INSTANCE.bind(3, schema, StructuredTimestamp.positiveInfinity(schema)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Non-finite structured temporal values require dialect-specific handling");
    }

    @Test
    @DisplayName("Should fail structured zoned timestamp infinity for Oracle")
    void shouldFailStructuredZonedTimestampInfinity() {
        final var schema = StructuredZonedTimestamp.schema();

        assertThatThrownBy(() -> StructuredZonedTimestampType.INSTANCE.bind(4, schema, StructuredZonedTimestamp.negativeInfinity(schema)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Non-finite structured temporal values require dialect-specific handling");
    }

    private ColumnDescriptor timeColumn(int precision) {
        return ColumnDescriptor.builder()
                .columnName("time_value")
                .jdbcType(Types.TIMESTAMP)
                .typeName("timestamp")
                .scale(precision)
                .build();
    }
}

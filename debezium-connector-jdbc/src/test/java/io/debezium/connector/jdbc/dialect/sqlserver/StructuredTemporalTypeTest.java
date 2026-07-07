/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.type.debezium.StructuredDurationType;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTime;
import io.debezium.time.StructuredZonedTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should cast structured time bindings to SQL Server time")
    void shouldCastStructuredTime() {
        final var schema = StructuredTime.schema();
        final var value = StructuredTime.from(schema, LocalTime.of(12, 13, 14, 123_456_789));

        final var bindings = StructuredTimeType.INSTANCE.bind(1, schema, value);

        assertThat(StructuredTimeType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as time(7))");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isInstanceOf(LocalDateTime.class);
        assertThat(bindings.get(0).getValue()).isEqualTo(LocalDateTime.of(LocalDate.EPOCH, LocalTime.of(12, 13, 14, 123_456_789)));
    }

    @Test
    @DisplayName("Should cast structured timestamp bindings to SQL Server datetime2")
    void shouldCastStructuredTimestamp() {
        final var schema = StructuredTimestamp.schema();

        assertThat(StructuredTimestampType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("cast(? as datetime2(7))");
    }

    @Test
    @DisplayName("Should cast structured zoned timestamp bindings to SQL Server datetimeoffset")
    void shouldCastStructuredZonedTimestamp() {
        final var schema = StructuredZonedTimestamp.schema();

        assertThat(StructuredZonedTimestampType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("cast(? as datetimeoffset(7))");
    }

    @Test
    @DisplayName("Should bind structured zoned time as SQL Server datetimeoffset value")
    void shouldBindStructuredZonedTime() {
        final var schema = StructuredZonedTime.schema();
        final var value = StructuredZonedTime.from(schema, OffsetTime.of(12, 13, 14, 123_456_789, ZoneOffset.ofHours(9)));

        final var bindings = StructuredZonedTimeType.INSTANCE.bind(2, schema, value);

        assertThat(StructuredZonedTimeType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as datetimeoffset(7))");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isInstanceOf(OffsetDateTime.class);
        assertThat(((OffsetDateTime) bindings.get(0).getValue()).toLocalDate()).isEqualTo(LocalDate.EPOCH);
        assertThat(((OffsetDateTime) bindings.get(0).getValue()).toOffsetTime())
                .isEqualTo(OffsetTime.of(12, 13, 14, 123_456_789, ZoneOffset.ofHours(9)));
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Test
    @DisplayName("Should bind structured duration as SQL Server string value")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 1, 2, 3, 4, 5, 6, 789_000_000);

        final var bindings = StructuredDurationType.INSTANCE.bind(4, schema, value);

        assertThat(StructuredDurationType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("?");
        assertThat(StructuredDurationType.INSTANCE.getDefaultValueBinding(schema, value))
                .isEqualTo("'1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds'");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    @Test
    @DisplayName("Should fail structured zoned timestamp infinity for SQL Server")
    void shouldFailStructuredZonedTimestampInfinity() {
        final var schema = StructuredZonedTimestamp.schema();

        assertThatThrownBy(() -> StructuredZonedTimestampType.INSTANCE.bind(3, schema, StructuredZonedTimestamp.positiveInfinity(schema)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Non-finite structured temporal values require dialect-specific handling");
    }
}

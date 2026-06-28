/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

import microsoft.sql.DateTimeOffset;

class SqlServerValueConvertersTest {

    private final SqlServerValueConverters converters = new SqlServerValueConverters(
            JdbcValueConverters.DecimalMode.PRECISE,
            TemporalPrecisionMode.STRUCTURED,
            CommonConnectorConfig.BinaryHandlingMode.BYTES);

    @Test
    void shouldPreserveStructuredDateRangeBoundaries() {
        final Column column = column("val_date", Types.DATE, "date", 0, null);
        final Field field = fieldFor(column);

        Struct value = (Struct) converters.converter(column, field).convert(LocalDate.of(1, 1, 1));

        assertThat(field.schema().name()).isEqualTo(StructuredDate.SCHEMA_NAME);
        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(1);
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 1);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 1);

        value = (Struct) converters.converter(column, field).convert(LocalDate.of(9999, 12, 31));

        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
    }

    @Test
    void shouldPreserveStructuredTimeSevenDigitPrecision() {
        final Column column = column("val_time", Types.TIME, "time", 0, 7);
        final Field field = fieldFor(column);

        final Struct value = (Struct) converters.converter(column, field)
                .convert(LocalTime.of(23, 59, 59, 999_999_900));

        assertThat(field.schema().name()).isEqualTo(StructuredTime.SCHEMA_NAME);
        assertThat(value.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(value.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_900);
    }

    @Test
    void shouldPreserveStructuredDatetime2BeyondNanoTimestampRange() {
        final Column column = column("val_datetime2", Types.TIMESTAMP, "datetime2", 0, 7);
        final Field field = fieldFor(column);

        final Struct value = (Struct) converters.converter(column, field)
                .convert(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_900));

        assertThat(field.schema().name()).isEqualTo(StructuredTimestamp.SCHEMA_NAME);
        assertThat(value.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isNull();
        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(value.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(value.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_900);
    }

    @Test
    void shouldPreserveStructuredDatetimeOffsetBeyondNanoTimestampRange() {
        final Column column = column("val_datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET, "datetimeoffset", 0, 7);
        final Field field = fieldFor(column);
        final OffsetDateTime timestamp = OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_900, ZoneOffset.ofHours(14));

        final Struct value = (Struct) converters.converter(column, field)
                .convert(DateTimeOffset.valueOf(Timestamp.from(timestamp.toInstant()), 14 * 60));

        assertThat(field.schema().name()).isEqualTo(StructuredZonedTimestamp.SCHEMA_NAME);
        assertThat(value.getString(StructuredTemporal.SPECIAL_VALUE_FIELD)).isNull();
        assertThat(value.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(value.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(value.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(value.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(value.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(value.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_900);
        assertThat(value.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)).isEqualTo(50_400);
    }

    private Field fieldFor(Column column) {
        final Schema schema = converters.schemaBuilder(column).optional().build();
        return new Field(column.name(), 0, schema);
    }

    private static Column column(String name, int jdbcType, String typeName, int length, Integer scale) {
        final var editor = Column.editor()
                .name(name)
                .type(typeName)
                .jdbcType(jdbcType)
                .length(length)
                .optional(true);
        if (scale != null) {
            editor.scale(scale);
        }
        return editor.create();
    }
}

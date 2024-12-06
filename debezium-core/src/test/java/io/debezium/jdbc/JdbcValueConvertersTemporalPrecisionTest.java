/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Test to test temporal to ISO 8601 String conversions.
 *
 * @author Ismail Simsek
 */
public class JdbcValueConvertersTemporalPrecisionTest {
    JdbcValueConverters converters = new JdbcValueConverters(null, TemporalPrecisionMode.ISOSTRING, ZoneOffset.UTC, null, null, null);
    //
    final Column dateCol = Column.editor().name("c1").type("DATE").optional(false).jdbcType(Types.DATE).create();
    final Field dateField = new Field(dateCol.name(), -1, converters.schemaBuilder(dateCol).build());
    final ValueConverter dateValConverter = converters.converter(dateCol, dateField);
    //
    final Column timeCol = Column.editor().name("c2").type("TIME").optional(false).jdbcType(Types.TIME).create();
    //
    final Column timestampCol = Column.editor().name("c2").type("TIMESTAMP").optional(false).jdbcType(Types.TIMESTAMP).create();

    @Test
    public void testSchemaBuilder() {
        // test schema types are correct! set as ZonedDate
        final Schema dateColSchema = converters.schemaBuilder(dateCol).schema();
        assertThat(dateColSchema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(dateColSchema.name()).isEqualTo("io.debezium.time.IsoDate");
        assertThat(dateColSchema.isOptional()).isEqualTo(false);
        // test schema types are correct! set as ZonedTime
        final Schema timeColSchema = converters.schemaBuilder(timeCol).schema();
        assertThat(timeColSchema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(timeColSchema.name()).isEqualTo("io.debezium.time.IsoTime");
        assertThat(dateColSchema.isOptional()).isEqualTo(false);
        // test schema types are correct! set as ZonedTimestamp
        final Schema tsColSchema = converters.schemaBuilder(timestampCol).schema();
        assertThat(tsColSchema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(tsColSchema.name()).isEqualTo("io.debezium.time.IsoTimestamp");
        assertThat(dateColSchema.isOptional()).isEqualTo(false);
    }

    @Test
    public void testIsoDate() throws ParseException {
        assertThat(dateCol.isOptional()).isEqualTo(false);

        assertThat(dateValConverter.convert(null)).isEqualTo("1970-01-01Z");
        //
        Object val = dateValConverter.convert(LocalDate.parse("2005-05-12"));
        assertThat(val).isEqualTo("2005-05-12Z");
        // LocalDateTime
        Object val2 = dateValConverter.convert(LocalDateTime.parse("2015-08-13T10:11:30"));
        assertThat(val2).isEqualTo("2015-08-13Z");
        // java.sql.Date
        Object val3 = dateValConverter.convert(java.sql.Date.valueOf("2005-05-14"));
        assertThat(val3).isEqualTo("2005-05-14Z");
        // Date
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); // Specify your date format
        Object val4 = dateValConverter.convert(formatter.parse("2005-05-15"));
        assertThat(val4).isEqualTo("2005-05-15Z");
        // LocalDate
        assertThat(dateValConverter.convert(LocalDate.ofEpochDay(16321))).isEqualTo("2014-09-08Z");
        assertThat(dateValConverter.convert(LocalDate.ofEpochDay(16321L))).isEqualTo("2014-09-08Z");
    }

    @Test
    public void testIsoTime() {
        Field timeField = new Field("tc1", -1, converters.schemaBuilder(timeCol).build());
        ValueConverter timeValConverter = converters.converter(timeCol, timeField);
        //
        assertThat(timeCol.isOptional()).isEqualTo(false);
        assertThat(timeValConverter.convert(null)).isEqualTo("00:00:00Z");
        // java.util.Date
        Object val = timeValConverter.convert(new java.util.Date(10, 30, 01, 3, 4, 5));
        assertThat(val).isEqualTo("03:04:05Z");
        // OffsetTime
        val = timeValConverter.convert(new java.sql.Time(10, 30, 01));
        assertThat(val).isEqualTo("10:30:01Z");
        // Duration
        Duration valDuration = Duration.ofHours(2).plusMinutes(30).plusNanos(5);
        val = timeValConverter.convert(valDuration);
        assertThat(val).isEqualTo("02:30:00.000000005Z");
        // LocalTime
        val = timeValConverter.convert(LocalTime.of(10, 30, 45, 123456789));
        assertThat(val).isEqualTo("10:30:45.123456789Z");
    }

    @Test
    public void testIsoTimestamp() {
        Field tsField = new Field("tsc1", -1, converters.schemaBuilder(timestampCol).build());
        ValueConverter tSValConverter = converters.converter(timestampCol, tsField);
        //
        assertThat(timestampCol.isOptional()).isEqualTo(false);
        assertThat(tSValConverter.convert(null)).isEqualTo("1970-01-01T00:00:00Z");
        // LocalDateTime
        Object val = tSValConverter.convert(LocalDateTime.parse("2011-01-11T16:40:30.123456789"));
        assertThat(val).isEqualTo("2011-01-11T16:40:30.123456789Z");
        // long milliseconds
        val = tSValConverter.convert(1732117483000L);
        assertThat(val).isEqualTo("2024-11-20T15:44:43Z");
    }

    @Test
    public void testMicroseconds() {
        JdbcValueConverters convertersMicro = new JdbcValueConverters(null, TemporalPrecisionMode.MICROSECONDS, ZoneOffset.UTC, null, null, null);
        JdbcValueConverters convertersNano = new JdbcValueConverters(null, TemporalPrecisionMode.NANOSECONDS, ZoneOffset.UTC, null, null, null);

        final Column timeCol = Column.editor().name("c2").type("TIME").optional(false).jdbcType(Types.TIME).create();
        Field timeField = new Field("time_col", -1, converters.schemaBuilder(timeCol).build());
        ValueConverter timeMicroValConverter = convertersMicro.converter(timeCol, timeField);
        ValueConverter timeNanoValConverter = convertersNano.converter(timeCol, timeField);
        // null
        assertThat(timeCol.isOptional()).isEqualTo(false);
        assertThat(timeMicroValConverter.convert(null)).isEqualTo(0L);
        assertThat(timeNanoValConverter.convert(null)).isEqualTo(0L);
        // java.sql.Time
        Object valMicro = timeMicroValConverter.convert(new java.sql.Time(10, 30, 01));
        Object valNano = timeNanoValConverter.convert(new java.sql.Time(10, 30, 01));
        assertThat(valMicro).isEqualTo(37801000000L);
        assertThat(valNano).isEqualTo(37801000000000L);
        // Duration
        Duration valDuration = Duration.ofHours(2).plusMinutes(30).plusMillis(4).plusNanos(5);
        assertThat(timeMicroValConverter.convert(valDuration)).isEqualTo(9000004000L);
        assertThat(timeNanoValConverter.convert(valDuration)).isEqualTo(9000004000005L);
    }
}

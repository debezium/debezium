/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 */
public abstract class BinlogValueConvertersTest<C extends SourceConnector> implements BinlogConnectorTest<C> {

    private static final TemporalAdjuster ADJUSTER = BinlogValueConverters::adjustTemporal;
    private static final byte[] INVALID_JSON = { 2, 1, 0, 91, 0, 0, 7, 0, 2, 0, 84, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28,
            0, 0, 47, 0, 116, 121, 112, 101 };

    @Test
    public void shouldAdjustLocalDateWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(00))).isEqualTo(localDateWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(01))).isEqualTo(localDateWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(10))).isEqualTo(localDateWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(69))).isEqualTo(localDateWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(70))).isEqualTo(localDateWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(71))).isEqualTo(localDateWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(99))).isEqualTo(localDateWithYear(1999));
    }

    @Test
    public void shouldAdjustLocalDateTimeWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(00))).isEqualTo(localDateTimeWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(01))).isEqualTo(localDateTimeWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(10))).isEqualTo(localDateTimeWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(69))).isEqualTo(localDateTimeWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(70))).isEqualTo(localDateTimeWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(71))).isEqualTo(localDateTimeWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(99))).isEqualTo(localDateTimeWithYear(1999));
    }

    @Test
    public void shouldNotAdjustLocalDateWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(-1))).isEqualTo(localDateWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(100))).isEqualTo(localDateWithYear(100));
    }

    @Test
    public void shouldNotAdjustLocalDateTimeWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(-1))).isEqualTo(localDateTimeWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(100))).isEqualTo(localDateTimeWithYear(100));
    }

    @Test
    public void testJsonValues() {
        String sql = "CREATE TABLE JSON_TABLE (" + "    A JSON," + "    B JSON NOT NULL" + ");";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "JSON_TABLE"));

        // ColA - Nullable column
        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).optional().build());
        assertThat(converters.converter(colA, fieldA).convert("{}")).isEqualTo("{}");
        assertThat(converters.converter(colA, fieldA).convert("[]")).isEqualTo("[]");
        assertThat(converters.converter(colA, fieldA).convert(new byte[0])).isNull();
        assertThat(converters.converter(colA, fieldA).convert(null)).isNull();
        assertThat(converters.converter(colA, fieldA).convert("{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }"))
                .isEqualTo("{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }");

        // ColB - NOT NUll column
        Column colB = table.columnWithName("B");
        Field fieldB = new Field(colB.name(), -1, converters.schemaBuilder(colB).build());
        assertThat(converters.converter(colB, fieldB).convert("{}")).isEqualTo("{}");
        assertThat(converters.converter(colB, fieldB).convert("[]")).isEqualTo("[]");
        assertThat(converters.converter(colB, fieldB).convert(new byte[0])).isEqualTo("{}");
        assertThat(converters.converter(colB, fieldB).convert(null)).isEqualTo("{}");
        assertThat(converters.converter(colB, fieldB).convert("{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }"))
                .isEqualTo("{ \"key1\": \"val1\", \"key2\": {\"key3\":\"val3\"} }");
    }

    @Test
    @FixFor({ "DBZ-2563", "DBZ-7143" })
    public void testSkipInvalidJsonValues() {
        String sql = "CREATE TABLE JSON_TABLE (" + "    A JSON," + "    B JSON NOT NULL" + ");";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        LogInterceptor logInterceptor = new LogInterceptor(converters.getClass());

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "JSON_TABLE"));

        // ColA - Nullable column
        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).optional().build());
        assertThat(converters.converter(colA, fieldA).convert(INVALID_JSON)).isEqualTo(null);
        assertThat(logInterceptor.containsWarnMessage("Failed to parse and read a JSON value on 'A JSON DEFAULT VALUE NULL'"))
                .describedAs("Expected null value of nullable column when parsing invalid json with WARN mode")
                .isTrue();

        // ColB - NOT NUll column
        Column colB = table.columnWithName("B");
        Field fieldB = new Field(colB.name(), -1, converters.schemaBuilder(colB).build());
        assertThat(converters.converter(colB, fieldB).convert(INVALID_JSON)).isEqualTo("{}");
        assertThat(logInterceptor.containsWarnMessage("Failed to parse and read a JSON value on 'B JSON NOT NULL'"))
                .describedAs("Expected '{}' value of non-null column when parsing invalid json with WARN mode")
                .isTrue();
    }

    @Test(expected = DebeziumException.class)
    @FixFor({ "DBZ-2563", "DBZ-7143" })
    public void testErrorOnInvalidJsonValues() {
        String sql = "CREATE TABLE JSON_TABLE (" + "    A JSON," + "    B JSON NOT NULL" + ");";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.FAIL);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "JSON_TABLE"));

        // ColA - Nullable column
        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).optional().build());
        converters.converter(colA, fieldA).convert(INVALID_JSON);
    }

    @Test
    @FixFor("DBC-3371")
    public void testFallbackDecimalValueScale() {
        int scale = 42;
        String sql = "CREATE TABLE DECIMAL_TABLE (A DECIMAL(3, " + scale + ") NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.CONNECT,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "DECIMAL_TABLE"));

        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).build());

        assertEquals(BigDecimal.ZERO.setScale(scale), converters.converter(colA, fieldA).convert(null));
    }

    @Test
    @FixFor("DBZ-5996")
    public void testZonedDateTimeWithMicrosecondPrecision() {
        String zonedDateTimeTable = "ZONED_DATE_TIME_TABLE";
        String sql = "CREATE TABLE " + zonedDateTimeTable + " (A TIMESTAMP(6) NOT NULL, B TIMESTAMP(3) NOT NULL, C TIMESTAMP(5) NOT NULL, D TIMESTAMP NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);

        Table table = tables.forTable(new TableId(null, null, zonedDateTimeTable));

        // Check with timestamp(6), output should always contain 6 digits in nanosecond part
        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).build());

        ValueConverter colAConverter = converters.converter(colA, fieldA);
        assertEquals("2023-01-11T00:34:10.000000Z", colAConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.000000Z")));
        assertEquals("2023-01-11T00:34:10.123456Z", colAConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123456Z")));
        assertEquals("2023-01-11T00:34:10.123000Z", colAConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123Z")));
        assertEquals("2023-01-11T00:34:10.000000Z", colAConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10Z")));

        // Check with timestamp(3), output should always contain 3 digits in nanosecond part
        Column colB = table.columnWithName("B");
        Field fieldB = new Field(colB.name(), -1, converters.schemaBuilder(colB).build());

        ValueConverter colBConverter = converters.converter(colB, fieldB);
        assertEquals("2023-01-11T00:34:10.000Z", colBConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.000000Z")));
        assertEquals("2023-01-11T00:34:10.123Z", colBConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.1234Z")));
        assertEquals("2023-01-11T00:34:10.123Z", colBConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123Z")));
        assertEquals("2023-01-11T00:34:10.010Z", colBConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.01Z")));
        assertEquals("2023-01-11T00:34:10.000Z", colBConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10Z")));

        // Check with timestamp(5), output should always contain 5 digits in nanosecond part
        Column colC = table.columnWithName("C");
        Field fieldC = new Field(colC.name(), -1, converters.schemaBuilder(colC).build());

        ValueConverter colCConverter = converters.converter(colC, fieldC);
        assertEquals("2023-01-11T00:34:10.00000Z", colCConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.000000Z")));
        assertEquals("2023-01-11T00:34:10.12345Z", colCConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.12345Z")));
        assertEquals("2023-01-11T00:34:10.12300Z", colCConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123Z")));
        assertEquals("2023-01-11T00:34:10.12345Z", colCConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123456Z")));
        assertEquals("2023-01-11T00:34:10.00000Z", colCConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10Z")));

        // Check with timestamp, output should always contain minimum number of digits in nanosecond part
        Column colD = table.columnWithName("D");
        Field fieldD = new Field(colD.name(), -1, converters.schemaBuilder(colD).build());

        ValueConverter colDConverter = converters.converter(colD, fieldD);
        assertEquals("2023-01-11T00:34:10Z", colDConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.000000Z")));
        assertEquals("2023-01-11T00:34:10.12345Z", colDConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.12345Z")));
        assertEquals("2023-01-11T00:34:10.123Z", colDConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123Z")));
        assertEquals("2023-01-11T00:34:10.123456Z", colDConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10.123456Z")));
        assertEquals("2023-01-11T00:34:10Z", colDConverter.convert(ZonedDateTime.parse("2023-01-11T00:34:10Z")));
    }

    @Test
    public void testInvalidLocalDate() {
        LogInterceptor interceptorInvalid = new LogInterceptor(BinlogValueConverters.class.getName() + ".invalid_value");
        String dateTable = "DATE_TABLE";
        String sql = "CREATE TABLE " + dateTable + " (A DATE NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);

        Table table = tables.forTable(new TableId(null, null, dateTable));
        Column colA = table.columnWithName("A");

        LocalDate actual = BinlogValueConverters.stringToLocalDate("0000-00-00", colA, table);
        assertThat(actual).isNull();

        assertThat(interceptorInvalid.containsWarnMessage("Invalid value")).isTrue();
    }

    @Test
    public void testDateValidYear() {
        String dateTable = "DATE_TABLE";
        String sql = "CREATE TABLE " + dateTable + " (A DATE NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);

        Table table = tables.forTable(new TableId(null, null, dateTable));
        Column colA = table.columnWithName("A");

        LocalDate actual = BinlogValueConverters.stringToLocalDate("0000-01-01", colA, table);
        assertThat(actual).isEqualTo(LocalDate.of(0, 1, 1));
    }

    @Test
    public void testInvalidTimestamp() {
        LogInterceptor interceptorInvalid = new LogInterceptor(BinlogValueConverters.class.getName() + ".invalid_value");
        String dateTable = "TIMESTAMP_TABLE";
        String sql = "CREATE TABLE " + dateTable + " (A TIMESTAMP(3) NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);

        Table table = tables.forTable(new TableId(null, null, dateTable));
        Column colA = table.columnWithName("A");

        String timestampString = "0000-00-00 00:00:00.000";

        assertThatThrownBy(() -> {
            Timestamp.valueOf(timestampString);
        }).isInstanceOf(RuntimeException.class);

        Boolean actual = BinlogValueConverters.containsZeroValuesInDatePart(timestampString, colA, table);
        assertThat(actual).isTrue();

        assertThat(interceptorInvalid.containsWarnMessage("Invalid value")).isTrue();
    }

    @Test
    public void testTimestampValidYear() {
        String dateTable = "TIMESTAMP_TABLE";
        String sql = "CREATE TABLE " + dateTable + " (A TIMESTAMP(3) NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);

        Table table = tables.forTable(new TableId(null, null, dateTable));
        Column colA = table.columnWithName("A");

        String timestampString = "0000-01-01 00:00:00.000";

        assertThat(Timestamp.valueOf(timestampString)).isNotNull();

        Boolean actual = BinlogValueConverters.containsZeroValuesInDatePart(timestampString, colA, table);
        assertThat(actual).isFalse();
    }

    protected LocalDate localDateWithYear(int year) {
        return LocalDate.of(year, Month.APRIL, 4);
    }

    protected LocalDateTime localDateTimeWithYear(int year) {
        return LocalDateTime.of(year, Month.APRIL, 4, 0, 0, 0);
    }

    protected abstract BinlogValueConverters getValueConverters(
                                                                JdbcValueConverters.DecimalMode decimalMode,
                                                                TemporalPrecisionMode temporalPrecisionMode,
                                                                JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                                BinaryHandlingMode binaryHandlingMode,
                                                                TemporalAdjuster temporalAdjuster,
                                                                EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode);

    protected abstract DdlParser getDdlParser();

}

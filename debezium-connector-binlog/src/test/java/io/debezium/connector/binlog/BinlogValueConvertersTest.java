/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.event.BinlogDateTimeValue;
import io.debezium.connector.binlog.event.BinlogDateValue;
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
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

/**
 * @author Randall Hauch
 */
public abstract class BinlogValueConvertersTest<C extends SourceConnector> implements BinlogConnectorTest<C> {

    private static final TemporalAdjuster ADJUSTER = BinlogValueConverters::adjustTemporal;
    private static final byte[] INVALID_JSON = { 2, 1, 0, 91, 0, 0, 7, 0, 2, 0, 84, 0, 18, 0, 4, 0, 22, 0, 6, 0, 12, 28,
            0, 0, 47, 0, 116, 121, 112, 101 };

    // Binary (binlog) representations of JSON values, in the MySQL binary JSON format

    // {"key1": "value1", "key2": "value2"}
    private static final byte[] BINARY_JSON_OBJECT = binaryJson(
            0x00, // small object
            0x02, 0x00, // element count
            0x28, 0x00, // size in bytes
            0x12, 0x00, 0x04, 0x00, // key "key1": offset, length
            0x16, 0x00, 0x04, 0x00, // key "key2": offset, length
            0x0c, 0x1a, 0x00, // value "value1": string, offset
            0x0c, 0x21, 0x00, // value "value2": string, offset
            'k', 'e', 'y', '1',
            'k', 'e', 'y', '2',
            0x06, 'v', 'a', 'l', 'u', 'e', '1',
            0x06, 'v', 'a', 'l', 'u', 'e', '2');

    // [1, 2]
    private static final byte[] BINARY_JSON_ARRAY = binaryJson(
            0x02, // small array
            0x02, 0x00, // element count
            0x0a, 0x00, // size in bytes
            0x05, 0x01, 0x00, // inlined int16 1
            0x05, 0x02, 0x00); // inlined int16 2

    // {"a": [1, 2], "b": {"c": "d"}}
    private static final byte[] BINARY_JSON_NESTED = binaryJson(
            0x00, // small object
            0x02, 0x00, // element count
            0x2c, 0x00, // size in bytes
            0x12, 0x00, 0x01, 0x00, // key "a": offset, length
            0x13, 0x00, 0x01, 0x00, // key "b": offset, length
            0x02, 0x14, 0x00, // value [1, 2]: small array, offset
            0x00, 0x1e, 0x00, // value {"c": "d"}: small object, offset
            'a',
            'b',
            0x02, 0x00, 0x0a, 0x00, // nested array header
            0x05, 0x01, 0x00,
            0x05, 0x02, 0x00,
            0x01, 0x00, 0x0e, 0x00, // nested object header
            0x0b, 0x00, 0x01, 0x00, // key "c": offset, length
            0x0c, 0x0c, 0x00, // value "d": string, offset
            'c',
            0x01, 'd');

    // {}
    private static final byte[] BINARY_JSON_EMPTY_OBJECT = binaryJson(0x00, 0x00, 0x00, 0x04, 0x00);

    // []
    private static final byte[] BINARY_JSON_EMPTY_ARRAY = binaryJson(0x02, 0x00, 0x00, 0x04, 0x00);

    // "tab\tand\nnewline"
    private static final byte[] BINARY_JSON_ESCAPED_SCALAR = binaryJson(
            0x0c, 0x0f, // string scalar, length
            't', 'a', 'b', 0x09, 'a', 'n', 'd', 0x0a, 'n', 'e', 'w', 'l', 'i', 'n', 'e');

    // 2015-01-15 23:24:25 as a DATETIME scalar
    private static final byte[] BINARY_JSON_DATETIME_SCALAR = binaryJsonPackedTemporal(
            0x0c, packDatetime(2015, 1, 15, 23, 24, 25), 0);

    // 23:24:25.12 as a TIME scalar
    private static final byte[] BINARY_JSON_TIME_SCALAR = binaryJsonPackedTemporal(
            0x0b, (23L << 12) | (24L << 6) | 25L, 120_000);

    // x'cafe' as an opaque scalar
    private static final byte[] BINARY_JSON_BLOB_SCALAR = binaryJson(
            0x0f, 0x0f, // opaque scalar, MYSQL_TYPE_VARCHAR
            0x02, // length
            0xca, 0xfe);

    @Test
    void shouldAdjustLocalDateWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(00))).isEqualTo(localDateWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(01))).isEqualTo(localDateWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(10))).isEqualTo(localDateWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(69))).isEqualTo(localDateWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(70))).isEqualTo(localDateWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(71))).isEqualTo(localDateWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(99))).isEqualTo(localDateWithYear(1999));
    }

    @Test
    void shouldAdjustLocalDateTimeWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(00))).isEqualTo(localDateTimeWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(01))).isEqualTo(localDateTimeWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(10))).isEqualTo(localDateTimeWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(69))).isEqualTo(localDateTimeWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(70))).isEqualTo(localDateTimeWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(71))).isEqualTo(localDateTimeWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(99))).isEqualTo(localDateTimeWithYear(1999));
    }

    @Test
    void shouldNotAdjustLocalDateWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(-1))).isEqualTo(localDateWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(100))).isEqualTo(localDateWithYear(100));
    }

    @Test
    void shouldNotAdjustLocalDateTimeWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(-1))).isEqualTo(localDateTimeWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(100))).isEqualTo(localDateTimeWithYear(100));
    }

    @Test
    void testJsonValues() {
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
    @FixFor("debezium/dbz#2376")
    public void testJsonValuesStreamingDiffersFromSnapshotWithLegacyFormattingMode() {
        ValueConverter converter = jsonColumnConverter(BinlogConnectorConfig.JsonStringFormattingMode.LEGACY);

        // Snapshot values are the server text, delivered unchanged
        assertThat(converter.convert("{\"key1\": \"value1\", \"key2\": \"value2\"}")).isEqualTo("{\"key1\": \"value1\", \"key2\": \"value2\"}");
        assertThat(converter.convert("[1, 2]")).isEqualTo("[1, 2]");

        // The same documents read from the binlog are serialized differently
        assertThat(converter.convert(BINARY_JSON_OBJECT)).isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
        assertThat(converter.convert(BINARY_JSON_ARRAY)).isEqualTo("[1,2]");
        assertThat(converter.convert(BINARY_JSON_NESTED)).isEqualTo("{\"a\":[1,2],\"b\":{\"c\":\"d\"}}");
        assertThat(converter.convert(BINARY_JSON_DATETIME_SCALAR)).isEqualTo("\"2015-01-15 23:24:25\"");
        assertThat(converter.convert(BINARY_JSON_TIME_SCALAR)).isEqualTo("\"23:24:25.12\"");
        assertThat(converter.convert(BINARY_JSON_BLOB_SCALAR)).isEqualTo("\"yv4=\"");
    }

    @Test
    @FixFor("debezium/dbz#2376")
    public void testJsonValuesStreamingMatchesSnapshotWithDatabaseFormattingMode() {
        ValueConverter converter = jsonColumnConverter(BinlogConnectorConfig.JsonStringFormattingMode.DATABASE);

        assertThat(converter.convert("{\"key1\": \"value1\", \"key2\": \"value2\"}")).isEqualTo("{\"key1\": \"value1\", \"key2\": \"value2\"}");
        assertThat(converter.convert("[1, 2]")).isEqualTo("[1, 2]");

        // Values read from the binlog now serialize to the strings the server produces
        assertThat(converter.convert(BINARY_JSON_OBJECT)).isEqualTo("{\"key1\": \"value1\", \"key2\": \"value2\"}");
        assertThat(converter.convert(BINARY_JSON_ARRAY)).isEqualTo("[1, 2]");
        assertThat(converter.convert(BINARY_JSON_NESTED)).isEqualTo("{\"a\": [1, 2], \"b\": {\"c\": \"d\"}}");
        assertThat(converter.convert(BINARY_JSON_DATETIME_SCALAR)).isEqualTo("\"2015-01-15 23:24:25.000000\"");
        assertThat(converter.convert(BINARY_JSON_TIME_SCALAR)).isEqualTo("\"23:24:25.120000\"");
        assertThat(converter.convert(BINARY_JSON_BLOB_SCALAR)).isEqualTo("\"base64:type15:yv4=\"");

        // Documents without entry separators are unaffected, and escaping is inherited unchanged
        assertThat(converter.convert(BINARY_JSON_EMPTY_OBJECT)).isEqualTo("{}");
        assertThat(converter.convert(BINARY_JSON_EMPTY_ARRAY)).isEqualTo("[]");
        assertThat(converter.convert(BINARY_JSON_ESCAPED_SCALAR)).isEqualTo("\"tab\\tand\\nnewline\"");

        // MariaDB replicates JSON as text, which is still passed through unchanged
        assertThat(converter.convert("{\"a\":2,\"b\": 3}".getBytes(StandardCharsets.UTF_8))).isEqualTo("{\"a\":2,\"b\": 3}");
    }

    private ValueConverter jsonColumnConverter(BinlogConnectorConfig.JsonStringFormattingMode jsonStringFormattingMode) {
        Configuration configuration = Configuration.create()
                .with(BinlogConnectorConfig.JSON_STRING_FORMATTING_MODE, jsonStringFormattingMode)
                .build();
        BinlogValueConverters converters = getValueConverters(configuration);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse("CREATE TABLE JSON_TABLE (A JSON);", tables);
        Table table = tables.forTable(new TableId(null, null, "JSON_TABLE"));

        Column column = table.columnWithName("A");
        Field field = new Field(column.name(), -1, converters.schemaBuilder(column).optional().build());
        return converters.converter(column, field);
    }

    private static byte[] binaryJson(int... bytes) {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }

    /**
     * Packs a datetime as MySQL does: 17 bits of {@code year * 13 + month}, then 5 bits each for the day
     * and hour and 6 bits each for the minute and second.
     */
    private static long packDatetime(int year, int month, int day, int hour, int min, int sec) {
        return (((long) year * 13 + month) << 22) | ((long) day << 17) | ((long) hour << 12) | ((long) min << 6) | sec;
    }

    /**
     * Builds a JSON temporal scalar: an opaque value holding the 8-byte little-endian packed temporal
     * value, whose lowest 24 bits are the microseconds.
     */
    private static byte[] binaryJsonPackedTemporal(int columnType, long packedValue, int microSeconds) {
        long raw = (packedValue << 24) | microSeconds;
        byte[] result = new byte[11];
        result[0] = 0x0f; // opaque scalar
        result[1] = (byte) columnType;
        result[2] = 0x08; // length
        for (int i = 0; i < 8; i++) {
            result[3 + i] = (byte) (raw >> (8 * i));
        }
        return result;
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

    @Test
    @FixFor({ "DBZ-2563", "DBZ-7143" })
    public void testErrorOnInvalidJsonValues() {
        assertThrows(DebeziumException.class, () -> {
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
        });
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
    public void testIntegerUnsignedSynonymMatchesIntUnsigned() {
        String sql = "CREATE TABLE INTEGER_UNSIGNED_TABLE (A INT UNSIGNED NOT NULL, B INTEGER UNSIGNED NOT NULL);";

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
        Table table = tables.forTable(new TableId(null, null, "INTEGER_UNSIGNED_TABLE"));

        // "INT UNSIGNED" and its "INTEGER UNSIGNED" synonym must produce identical schemas/conversions,
        // otherwise streaming (DDL-parsed) and snapshot (JDBC metadata, always normalized to "INT") diverge.
        Column colA = table.columnWithName("A");
        Column colB = table.columnWithName("B");

        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).build());
        Field fieldB = new Field(colB.name(), -1, converters.schemaBuilder(colB).build());

        assertEquals(fieldA.schema(), fieldB.schema());
        assertEquals(org.apache.kafka.connect.data.Schema.Type.INT64, fieldB.schema().type());

        long valueAboveIntMax = 4294967295L;
        assertEquals(valueAboveIntMax, converters.converter(colB, fieldB).convert(valueAboveIntMax));
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
    void testInvalidLocalDate() {
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
    void testDateValidYear() {
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
    void testInvalidTimestamp() {
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
    void testTimestampValidYear() {
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

    @Test
    void shouldUseStructuredTemporalMode() {
        String tableName = "TEMPORAL_TABLE";
        String sql = "CREATE TABLE " + tableName + " (D DATE NOT NULL, DT DATETIME(6) NOT NULL, TS TIMESTAMP(6) NOT NULL, T TIME(6) NOT NULL);";

        final BinlogValueConverters converters = getValueConverters(
                JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.STRUCTURED,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);

        DdlParser parser = getDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, tableName));

        Column dateColumn = table.columnWithName("D");
        Field dateField = new Field(dateColumn.name(), -1, converters.schemaBuilder(dateColumn).build());
        assertThat(dateField.schema().name()).isEqualTo(StructuredDate.SCHEMA_NAME);
        Struct date = (Struct) converters.converter(dateColumn, dateField).convert(new BinlogDateValue(0, 0, 0));
        assertThat(date.getInt32(StructuredTemporal.YEAR_FIELD)).isZero();
        assertThat(date.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 0);
        assertThat(date.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 0);
        date = (Struct) converters.converter(dateColumn, dateField).convert(new BinlogDateValue(9999, 12, 31));
        assertThat(date.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(date.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(date.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);

        Column datetimeColumn = table.columnWithName("DT");
        Field datetimeField = new Field(datetimeColumn.name(), -1, converters.schemaBuilder(datetimeColumn).build());
        assertThat(datetimeField.schema().name()).isEqualTo(StructuredTimestamp.SCHEMA_NAME);
        Struct datetime = (Struct) converters.converter(datetimeColumn, datetimeField).convert(new BinlogDateTimeValue(2026, 2, 31, 12, 13, 14, 123_456_000));
        assertThat(datetime.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(2026);
        assertThat(datetime.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 2);
        assertThat(datetime.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(datetime.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(123_456_000);
        datetime = (Struct) converters.converter(datetimeColumn, datetimeField).convert(new BinlogDateTimeValue(9999, 12, 31, 23, 59, 59, 999_999_000));
        assertThat(datetime.getInt32(StructuredTemporal.YEAR_FIELD)).isEqualTo(9999);
        assertThat(datetime.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 12);
        assertThat(datetime.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 31);
        assertThat(datetime.getInt8(StructuredTemporal.HOUR_FIELD)).isEqualTo((byte) 23);
        assertThat(datetime.getInt8(StructuredTemporal.MINUTE_FIELD)).isEqualTo((byte) 59);
        assertThat(datetime.getInt8(StructuredTemporal.SECOND_FIELD)).isEqualTo((byte) 59);
        assertThat(datetime.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_000);

        Column timestampColumn = table.columnWithName("TS");
        Field timestampField = new Field(timestampColumn.name(), -1, converters.schemaBuilder(timestampColumn).build());
        assertThat(timestampField.schema().name()).isEqualTo(StructuredZonedTimestamp.SCHEMA_NAME);
        Struct timestamp = (Struct) converters.converter(timestampColumn, timestampField).convert(new BinlogDateTimeValue(0, 0, 0, 0, 0, 0, 0));
        assertThat(timestamp.getInt32(StructuredTemporal.YEAR_FIELD)).isZero();
        assertThat(timestamp.getInt8(StructuredTemporal.MONTH_FIELD)).isEqualTo((byte) 0);
        assertThat(timestamp.getInt8(StructuredTemporal.DAY_FIELD)).isEqualTo((byte) 0);
        assertThat(timestamp.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD)).isZero();

        Column timeColumn = table.columnWithName("T");
        Field timeField = new Field(timeColumn.name(), -1, converters.schemaBuilder(timeColumn).build());
        assertThat(timeField.schema().name()).isEqualTo(StructuredDuration.SCHEMA_NAME);
        Struct duration = (Struct) converters.converter(timeColumn, timeField).convert(Duration.ofHours(-13).minusMinutes(14).minusSeconds(15).minusNanos(123_456_000));
        assertThat(duration.getInt32(StructuredTemporal.HOURS_FIELD)).isEqualTo(-13);
        assertThat(duration.getInt32(StructuredTemporal.MINUTES_FIELD)).isEqualTo(-14);
        assertThat(duration.getInt64(StructuredTemporal.SECONDS_FIELD)).isEqualTo(-15L);
        assertThat(duration.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(-123_456_000);
        duration = (Struct) converters.converter(timeColumn, timeField).convert(Duration.ofHours(838).plusMinutes(59).plusSeconds(59).plusNanos(999_999_000));
        assertThat(duration.getInt32(StructuredTemporal.HOURS_FIELD)).isEqualTo(838);
        assertThat(duration.getInt32(StructuredTemporal.MINUTES_FIELD)).isEqualTo(59);
        assertThat(duration.getInt64(StructuredTemporal.SECONDS_FIELD)).isEqualTo(59L);
        assertThat(duration.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(999_999_000);
        duration = (Struct) converters.converter(timeColumn, timeField).convert(Duration.ofHours(-838).minusMinutes(59).minusSeconds(59).minusNanos(999_999_000));
        assertThat(duration.getInt32(StructuredTemporal.HOURS_FIELD)).isEqualTo(-838);
        assertThat(duration.getInt32(StructuredTemporal.MINUTES_FIELD)).isEqualTo(-59);
        assertThat(duration.getInt64(StructuredTemporal.SECONDS_FIELD)).isEqualTo(-59L);
        assertThat(duration.getInt32(StructuredTemporal.NANOS_FIELD)).isEqualTo(-999_999_000);
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

    protected abstract BinlogValueConverters getValueConverters(Configuration configuration);

    protected abstract DdlParser getDdlParser();

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogValueConvertersTest;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 *
 */
public class MySqlValueConvertersTest extends BinlogValueConvertersTest<MySqlConnector> implements MySqlCommon {

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

    @Override
    protected BinlogValueConverters getValueConverters(JdbcValueConverters.DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode,
                                                       TemporalAdjuster temporalAdjuster,
                                                       EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                temporalAdjuster,
                eventConvertingFailureHandlingMode);
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MySqlAntlrDdlParser();
    }

    @Test
    @FixFor("debezium/dbz#2376")
    public void testJsonValuesInLegacyFormattingMode() {
        ValueConverter converter = jsonColumnConverter(MySqlConnectorConfig.JsonStringFormattingMode.LEGACY);

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
    public void testJsonValuesInDatabaseFormattingMode() {
        ValueConverter converter = jsonColumnConverter(MySqlConnectorConfig.JsonStringFormattingMode.DATABASE);

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

        // A value that is not binary JSON falls back to the base implementation, which passes it through
        assertThat(converter.convert("{\"a\":2,\"b\": 3}".getBytes(StandardCharsets.UTF_8))).isEqualTo("{\"a\":2,\"b\": 3}");
    }

    private ValueConverter jsonColumnConverter(MySqlConnectorConfig.JsonStringFormattingMode jsonStringFormattingMode) {
        Configuration configuration = Configuration.create()
                .with(MySqlConnectorConfig.JSON_STRING_FORMATTING_MODE, jsonStringFormattingMode)
                .build();
        MySqlValueConverters converters = new MySqlValueConvertersFactory().create(configuration, x -> x);

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
}

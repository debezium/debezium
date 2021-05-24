/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.TemporalAdjuster;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Field;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 *
 */
public class MySqlValueConvertersTest {

    private static final TemporalAdjuster ADJUSTER = MySqlValueConverters::adjustTemporal;
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

        MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT, JdbcValueConverters.BigIntUnsignedMode.LONG, BinaryHandlingMode.BYTES);

        DdlParser parser = new MySqlAntlrDdlParser();
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
    @FixFor("DBZ-2563")
    public void testSkipInvalidJsonValues() {
        final AtomicInteger errorCount = new AtomicInteger(0);
        String sql = "CREATE TABLE JSON_TABLE (" + "    A JSON," + "    B JSON NOT NULL" + ");";

        MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT, JdbcValueConverters.BigIntUnsignedMode.LONG, BinaryHandlingMode.BYTES,
                x -> x, (message, exception) -> {
                    errorCount.incrementAndGet();
                });

        DdlParser parser = new MySqlAntlrDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "JSON_TABLE"));

        // ColA - Nullable column
        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).optional().build());
        assertThat(converters.converter(colA, fieldA).convert(INVALID_JSON)).isEqualTo(null);
        assertThat(errorCount.get()).isEqualTo(1);

        // ColB - NOT NUll column
        Column colB = table.columnWithName("B");
        Field fieldB = new Field(colB.name(), -1, converters.schemaBuilder(colB).build());
        assertThat(converters.converter(colB, fieldB).convert(INVALID_JSON)).isEqualTo("{}");
        assertThat(errorCount.get()).isEqualTo(2);
    }

    @Test(expected = DebeziumException.class)
    @FixFor("DBZ-2563")
    public void testErrorOnInvalidJsonValues() {
        String sql = "CREATE TABLE JSON_TABLE (" + "    A JSON," + "    B JSON NOT NULL" + ");";

        MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT, JdbcValueConverters.BigIntUnsignedMode.LONG, BinaryHandlingMode.BYTES,
                x -> x, (message, exception) -> {
                    throw new DebeziumException(message, exception);
                });

        DdlParser parser = new MySqlAntlrDdlParser();
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

        MySqlValueConverters converters = new MySqlValueConverters(JdbcValueConverters.DecimalMode.PRECISE,
                TemporalPrecisionMode.CONNECT, JdbcValueConverters.BigIntUnsignedMode.LONG, BinaryHandlingMode.BYTES,
                x -> x, (message, exception) -> {
                    throw new DebeziumException(message, exception);
                });

        DdlParser parser = new MySqlAntlrDdlParser();
        Tables tables = new Tables();
        parser.parse(sql, tables);
        Table table = tables.forTable(new TableId(null, null, "DECIMAL_TABLE"));

        Column colA = table.columnWithName("A");
        Field fieldA = new Field(colA.name(), -1, converters.schemaBuilder(colA).build());

        assertEquals(BigDecimal.ZERO.setScale(scale), converters.converter(colA, fieldA).convert(null));
    }

    protected LocalDate localDateWithYear(int year) {
        return LocalDate.of(year, Month.APRIL, 4);
    }

    protected LocalDateTime localDateTimeWithYear(int year) {
        return LocalDateTime.of(year, Month.APRIL, 4, 0, 0, 0);
    }

}

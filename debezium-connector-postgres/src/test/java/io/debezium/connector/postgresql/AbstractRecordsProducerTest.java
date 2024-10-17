/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.postgresql.jdbc.PgStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.postgresql.data.Ltree;
import io.debezium.data.Bits;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.SchemaUtil;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.junit.TestLogger;
import io.debezium.relational.TableId;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Testing;

/**
 * Base class for the integration tests for the different {@link RecordsProducer} instances
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public abstract class AbstractRecordsProducerTest extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRecordsProducerTest.class);

    @Rule
    public TestRule logTestName = new TestLogger(LOGGER);

    protected static final Pattern INSERT_TABLE_MATCHING_PATTERN = Pattern.compile("insert into (.*)\\(.*\\) VALUES .*", Pattern.CASE_INSENSITIVE);
    protected static final Pattern DELETE_TABLE_MATCHING_PATTERN = Pattern.compile("delete from (.*) where .*", Pattern.CASE_INSENSITIVE);

    protected static final String INSERT_CASH_TYPES_STMT = "INSERT INTO cash_table (csh) VALUES ('$1234.11')";
    protected static final String INSERT_NEGATIVE_CASH_TYPES_STMT = "INSERT INTO cash_table (csh) VALUES ('($1234.11)')";
    protected static final String INSERT_NULL_CASH_TYPES_STMT = "INSERT INTO cash_table (csh) VALUES (NULL)";
    protected static final String INSERT_DATE_TIME_TYPES_STMT = "INSERT INTO time_table(ts, tsneg, ts_ms, ts_us, tz, date, date_pinf, date_ninf, ti, tip, ttf, ttz, tptz, it, ts_large, ts_large_us, ts_large_ms, tz_large, ts_max, ts_min, tz_max, tz_min, ts_pinf, ts_ninf, tz_pinf, tz_ninf, tz_zero) "
            +
            "VALUES ('2016-11-04T13:51:30.123456'::TIMESTAMP, '1936-10-25T22:10:12.608'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456+02:00'::TIMESTAMPTZ, "
            +
            "'2016-11-04'::DATE, 'infinity'::DATE, '-infinity'::DATE, '13:51:30'::TIME, '13:51:30.123'::TIME, '24:00:00'::TIME, '13:51:30.123789+02:00'::TIMETZ, '13:51:30.123+02:00'::TIMETZ, "
            +
            "'P1Y2M3DT4H5M6.78S'::INTERVAL," +
            "'21016-11-04T13:51:30.123456'::TIMESTAMP, '21016-11-04T13:51:30.123457'::TIMESTAMP, '21016-11-04T13:51:30.124'::TIMESTAMP," +
            "'21016-11-04T13:51:30.123456+07:00'::TIMESTAMPTZ," +
            "'294247-01-01T23:59:59.999999'::TIMESTAMP," +
            "'4713-12-31T23:59:59.999999 BC'::TIMESTAMP," +
            "'294247-01-01T23:59:59.999999+00:00'::TIMESTAMPTZ," +
            "'4714-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ," +
            "'infinity'::TIMESTAMP," +
            "'-infinity'::TIMESTAMP," +
            "'infinity'::TIMESTAMPTZ," +
            "'-infinity'::TIMESTAMPTZ," +
            "'21016-11-04T13:51:30.000000+07:00'::TIMESTAMPTZ"
            + ")";
    protected static final String INSERT_BIN_TYPES_STMT = "INSERT INTO bitbin_table (ba, bol, bol2, bs, bs7, bv, bv2, bvl, bvunlimited1, bvunlimited2) " +
            "VALUES (E'\\\\001\\\\002\\\\003'::bytea, '0'::bit(1), '1'::bit(1), '11'::bit(2), '1'::bit(7), '00'::bit(2), '000000110000001000000001'::bit(24)," +
            "'1000000000000000000000000000000000000000000000000000000000000000'::bit(64), '101', '111011010001000110000001000000001')";
    protected static final String INSERT_BYTEA_BINMODE_STMT = "INSERT INTO bytea_binmode_table (ba, bytea_array) VALUES (E'\\\\001\\\\002\\\\003'::bytea, array[E'\\\\000\\\\001\\\\002'::bytea, E'\\\\003\\\\004\\\\005'::bytea])";
    protected static final String INSERT_CIRCLE_STMT = "INSERT INTO circle_table (ccircle) VALUES ('((10, 20),10)'::circle)";
    protected static final String INSERT_GEOM_TYPES_STMT = "INSERT INTO geom_table(p) VALUES ('(1,1)'::point)";
    protected static final String INSERT_TEXT_TYPES_STMT = "INSERT INTO text_table(j, jb, x, u) " +
            "VALUES ('{\"bar\": \"baz\"}'::json, '{\"bar\": \"baz\"}'::jsonb, " +
            "'<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID)";
    protected static final String INSERT_STRING_TYPES_STMT = "INSERT INTO string_table (vc, vcv, ch, c, t, b, bnn, ct) " +
            "VALUES ('\u017E\u0161', 'bb', 'cdef', 'abc', 'some text', E'\\\\000\\\\001\\\\002'::bytea, E'\\\\003\\\\004\\\\005'::bytea, 'Hello World')";
    protected static final String INSERT_NETWORK_ADDRESS_TYPES_STMT = "INSERT INTO network_address_table (i) " +
            "VALUES ('192.168.2.0/12')";
    protected static final String INSERT_CIDR_NETWORK_ADDRESS_TYPE_STMT = "INSERT INTO cidr_network_address_table (i) " +
            "VALUES ('192.168.100.128/25');";
    protected static final String INSERT_MACADDR_TYPE_STMT = "INSERT INTO macaddr_table (m) " +
            "VALUES ('08:00:2b:01:02:03');";
    protected static final String INSERT_MACADDR8_TYPE_STMT = "INSERT INTO macaddr8_table (m) " +
            "VALUES ('08:00:2b:01:02:03:04:05');";
    protected static final String INSERT_NUMERIC_TYPES_STMT = "INSERT INTO numeric_table (si, i, bi, r, db, r_int, db_int, r_nan, db_nan, r_pinf, db_pinf, r_ninf, db_ninf, ss, bs, b, o) "
            +
            "VALUES (1, 123456, 1234567890123, 3.3, 4.44, 3, 4, 'NaN', 'NaN', 'Infinity', 'Infinity', '-Infinity', '-Infinity', 1, 123, true, 4000000000)";

    protected static final String INSERT_NUMERIC_DECIMAL_TYPES_STMT = "INSERT INTO numeric_decimal_table (d, dzs, dvs, d_nn, n, nzs, nvs, "
            + "d_int, dzs_int, dvs_int, n_int, nzs_int, nvs_int, "
            + "d_nan, dzs_nan, dvs_nan, n_nan, nzs_nan, nvs_nan"
            + ") "
            + "VALUES (1.1, 10.11, 10.1111, 3.30, 22.22, 22.2, 22.2222, "
            + "1, 10, 10, 22, 22, 22, "
            + "'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN'"
            + ")";

    protected static final String INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN = "INSERT INTO numeric_decimal_table (d, dzs, dvs, d_nn, n, nzs, nvs, "
            + "d_int, dzs_int, dvs_int, n_int, nzs_int, nvs_int, "
            + "d_nan, dzs_nan, dvs_nan, n_nan, nzs_nan, nvs_nan"
            + ") "
            + "VALUES (1.1, 10.11, 10.1111, 3.30, 22.22, 22.2, 22.2222, "
            + "1, 10, 10, 22, 22, 22, "
            + "null, null, null, null, null, null"
            + ")";

    protected static final String INSERT_NUMERIC_DECIMAL_TYPES_STMT_WITH_INFINITY = "INSERT INTO numeric_decimal_table (d, dzs, dvs, d_nn, n, nzs, nvs, "
            + "d_int, dzs_int, dvs_int, n_int, nzs_int, nvs_int, "
            + "d_nan, dzs_nan, dvs_nan, n_nan, nzs_nan, nvs_nan"
            + ") "
            + "VALUES (1.1, 10.11, 10.1111, 3.30, 22.22, 22.2, 22.2222, "
            + "1, 10, 10, 22, 22, 22, "
            + "null, null, 'Infinity', null, null, '-Infinity'"
            + ")";

    protected static final String INSERT_RANGE_TYPES_STMT = "INSERT INTO range_table (unbounded_exclusive_tsrange, bounded_inclusive_tsrange, unbounded_exclusive_tstzrange, bounded_inclusive_tstzrange, unbounded_exclusive_daterange, bounded_exclusive_daterange, int4_number_range, numerange, int8_number_range) "
            +
            "VALUES ('[2019-03-31 15:30:00, infinity)', '[2019-03-31 15:30:00, 2019-04-30 15:30:00]', '[2017-06-05 11:29:12.549426+00,)', '[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]', '[2019-03-31, infinity)', '[2019-03-31, 2019-04-30)', '[1000,6000)', '[5.3,6.3)', '[1000000,6000000)')";

    protected static final String INSERT_ARRAY_TYPES_STMT = "INSERT INTO array_table (int_array, bigint_array, text_array, char_array, varchar_array, date_array, numeric_array, varnumeric_array, citext_array, inet_array, cidr_array, macaddr_array, tsrange_array, tstzrange_array, daterange_array, int4range_array, numerange_array, int8range_array, uuid_array, json_array, jsonb_array, oid_array) "
            +
            "VALUES ('{1,2,3}', '{1550166368505037572}', '{\"one\",\"two\",\"three\"}', '{\"cone\",\"ctwo\",\"cthree\"}', '{\"vcone\",\"vctwo\",\"vcthree\"}', '{2016-11-04,2016-11-05,2016-11-06}', '{1.2,3.4,5.6}', '{1.1,2.22,3.333}', '{\"four\",\"five\",\"six\"}', '{\"192.168.2.0/12\",\"192.168.1.1\",\"192.168.0.2/1\"}', '{\"192.168.100.128/25\", \"192.168.0.0/25\", \"192.168.1.0/24\"}', '{\"08:00:2b:01:02:03\", \"08-00-2b-01-02-03\", \"08002b:010203\"}',"
            +
            "'{\"[2019-03-31 15:30:00, infinity)\", \"[2019-03-31 15:30:00, 2019-04-30 15:30:00]\"}', '{\"[2017-06-05 11:29:12.549426+00,)\", \"[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]\"}', '{\"[2019-03-31, infinity)\", \"[2019-03-31, 2019-04-30)\"}', '{\"[1,6)\", \"[1,4)\"}', '{\"[5.3,6.3)\", \"[10.0,20.0)\"}', '{\"[1000000,6000000)\", \"[5000,9000)\"}', '{\"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\", \"f0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\"}',"
            +
            "array['{\"bar\": \"baz\"}','{\"foo\": \"qux\"}']::json[], array['{\"bar\": \"baz\"}','{\"foo\": \"qux\"}']::jsonb[], '{3,4000000000}')";

    protected static final String INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT = "INSERT INTO array_table_with_nulls (int_array, bigint_array, text_array, date_array, numeric_array, varnumeric_array, citext_array, inet_array, cidr_array, macaddr_array, tsrange_array, tstzrange_array, daterange_array, int4range_array, numerange_array, int8range_array, uuid_array, json_array, jsonb_array) "
            +
            "VALUES (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)";

    protected static final String INSERT_POSTGIS_TYPES_STMT = "INSERT INTO public.postgis_table (p, ml) " +
            "VALUES ('SRID=3187;POINT(174.9479 -36.7208)'::postgis.geometry, 'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::postgis.geography)";

    protected static final String INSERT_POSTGIS_TYPES_IN_PUBLIC_STMT = "INSERT INTO public.postgis_table (p, ml) " +
            "VALUES ('SRID=3187;POINT(174.9479 -36.7208)'::geometry, 'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography)";

    protected static final String INSERT_POSTGIS_ARRAY_TYPES_STMT = "INSERT INTO public.postgis_array_table (ga, gann) " +
            "VALUES (" +
            "ARRAY['GEOMETRYCOLLECTION EMPTY'::postgis.geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::postgis.geometry], "
            +
            "ARRAY['GEOMETRYCOLLECTION EMPTY'::postgis.geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::postgis.geometry]"
            +
            ")";

    protected static final String INSERT_POSTGIS_ARRAY_TYPES_IN_PUBLIC_STMT = "INSERT INTO public.postgis_array_table (ga, gann) " +
            "VALUES (" +
            "ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::geometry], " +
            "ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::geometry]" +
            ")";

    protected static final String INSERT_QUOTED_TYPES_STMT = "INSERT INTO \"Quoted_\"\" . Schema\".\"Quoted_\"\" . Table\" (\"Quoted_\"\" . Text_Column\") " +
            "VALUES ('some text')";

    protected static final String INSERT_CUSTOM_TYPES_STMT = "INSERT INTO custom_table (lt, i, n, lt_array) " +
            "VALUES ('Top.Collections.Pictures.Astronomy.Galaxies', '978-0-393-04002-9', NULL, '{\"Ship.Frigate\",\"Ship.Destroyer\"}')";

    protected static final String INSERT_HSTORE_TYPE_STMT = "INSERT INTO hstore_table (hs) VALUES ('\"key\" => \"val\"'::hstore)";

    protected static final String INSERT_HSTORE_TYPE_WITH_MULTIPLE_VALUES_STMT = "INSERT INTO hstore_table_mul (hs, hsarr) VALUES (" +
            "'\"key1\" => \"val1\",\"key2\" => \"val2\",\"key3\" => \"val3\"', " +
            "array['\"key4\" => \"val4\",\"key5\" => NULL'::hstore, '\"key6\" => \"val6\"']" +
            ")";

    protected static final String INSERT_HSTORE_TYPE_WITH_NULL_VALUES_STMT = "INSERT INTO hstore_table_with_null (hs) VALUES ('\"key1\" => \"val1\",\"key2\" => NULL')";

    protected static final String INSERT_HSTORE_TYPE_WITH_SPECIAL_CHAR_STMT = "INSERT INTO hstore_table_with_special (hs) VALUES ('\"key_#1\" => \"val 1\",\"key 2\" =>\" ##123 78\"')";

    protected static final String DELETE_DATE_TIME_TYPES_STMT = "DELETE FROM time_table WHERE pk=1;";

    protected static final Set<String> ALL_STMTS = new HashSet<>(Arrays.asList(INSERT_NUMERIC_TYPES_STMT, INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN,
            INSERT_DATE_TIME_TYPES_STMT, INSERT_BIN_TYPES_STMT, INSERT_GEOM_TYPES_STMT, INSERT_TEXT_TYPES_STMT,
            INSERT_CASH_TYPES_STMT, INSERT_STRING_TYPES_STMT, INSERT_CIDR_NETWORK_ADDRESS_TYPE_STMT,
            INSERT_NETWORK_ADDRESS_TYPES_STMT, INSERT_MACADDR_TYPE_STMT,
            INSERT_ARRAY_TYPES_STMT, INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT, INSERT_QUOTED_TYPES_STMT,
            INSERT_POSTGIS_TYPES_STMT, INSERT_POSTGIS_ARRAY_TYPES_STMT));

    protected List<SchemaAndValueField> schemasAndValuesForNumericType() {
        final List<SchemaAndValueField> fields = new ArrayList<SchemaAndValueField>();

        fields.addAll(Arrays.asList(new SchemaAndValueField("si", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                new SchemaAndValueField("i", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123456),
                new SchemaAndValueField("bi", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 1234567890123L),
                new SchemaAndValueField("r", Schema.OPTIONAL_FLOAT32_SCHEMA, 3.3f),
                new SchemaAndValueField("db", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.44d),
                new SchemaAndValueField("r_int", Schema.OPTIONAL_FLOAT32_SCHEMA, 3.0f),
                new SchemaAndValueField("db_int", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.0d),
                new SchemaAndValueField("ss", SchemaBuilder.int16().defaultValue((short) 0).build(), (short) 1),
                new SchemaAndValueField("bs", SchemaBuilder.int64().defaultValue(0L).build(), 123L),
                new SchemaAndValueField("b", Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.TRUE),
                new SchemaAndValueField("o", Schema.OPTIONAL_INT64_SCHEMA, 4_000_000_000L),
                new SchemaAndValueField("r_nan", Schema.OPTIONAL_FLOAT32_SCHEMA, Float.NaN),
                new SchemaAndValueField("db_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("r_pinf", Schema.OPTIONAL_FLOAT32_SCHEMA, Float.POSITIVE_INFINITY),
                new SchemaAndValueField("db_pinf", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.POSITIVE_INFINITY),
                new SchemaAndValueField("r_ninf", Schema.OPTIONAL_FLOAT32_SCHEMA, Float.NEGATIVE_INFINITY),
                new SchemaAndValueField("db_ninf", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NEGATIVE_INFINITY)));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForBigDecimalEncodedNumericTypes() {
        final Struct dvs = new Struct(VariableScaleDecimal.schema());
        dvs.put("scale", 4).put("value", new BigDecimal("10.1111").unscaledValue().toByteArray());
        final Struct nvs = new Struct(VariableScaleDecimal.schema());
        nvs.put("scale", 4).put("value", new BigDecimal("22.2222").unscaledValue().toByteArray());
        final Struct dvs_int = new Struct(VariableScaleDecimal.schema());
        dvs_int.put("scale", 0).put("value", new BigDecimal("10").unscaledValue().toByteArray());
        final Struct nvs_int = new Struct(VariableScaleDecimal.schema());
        nvs_int.put("scale", 0).put("value", new BigDecimal("22").unscaledValue().toByteArray());
        final List<SchemaAndValueField> fields = new ArrayList<SchemaAndValueField>(Arrays.asList(
                new SchemaAndValueField("d", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "3").optional().build(), new BigDecimal("1.10")),
                new SchemaAndValueField("dzs", Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "4").optional().build(), new BigDecimal("10")),
                new SchemaAndValueField("dvs", VariableScaleDecimal.optionalSchema(), dvs),
                new SchemaAndValueField("d_nn", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "3").build(), new BigDecimal("3.30")),
                new SchemaAndValueField("n", Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "6").optional().build(), new BigDecimal("22.2200")),
                new SchemaAndValueField("nzs", Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "4").optional().build(), new BigDecimal("22")),
                new SchemaAndValueField("nvs", VariableScaleDecimal.optionalSchema(), nvs),
                new SchemaAndValueField("d_int", Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "3").optional().build(), new BigDecimal("1.00")),
                new SchemaAndValueField("dvs_int", VariableScaleDecimal.optionalSchema(), dvs_int),
                new SchemaAndValueField("n_int", Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "6").optional().build(), new BigDecimal("22.0000")),
                new SchemaAndValueField("nvs_int", VariableScaleDecimal.optionalSchema(), nvs_int)));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringEncodedNumericTypes() {
        final List<SchemaAndValueField> fields = new ArrayList<SchemaAndValueField>(Arrays.asList(
                new SchemaAndValueField("d", Schema.OPTIONAL_STRING_SCHEMA, "1.10"),
                new SchemaAndValueField("dzs", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("dvs", Schema.OPTIONAL_STRING_SCHEMA, "10.1111"),
                new SchemaAndValueField("n", Schema.OPTIONAL_STRING_SCHEMA, "22.2200"),
                new SchemaAndValueField("nzs", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("nvs", Schema.OPTIONAL_STRING_SCHEMA, "22.2222"),
                new SchemaAndValueField("d_int", Schema.OPTIONAL_STRING_SCHEMA, "1.00"),
                new SchemaAndValueField("dzs_int", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("dvs_int", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("n_int", Schema.OPTIONAL_STRING_SCHEMA, "22.0000"),
                new SchemaAndValueField("nzs_int", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("nvs_int", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("d_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN"),
                new SchemaAndValueField("dzs_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN"),
                new SchemaAndValueField("dvs_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN"),
                new SchemaAndValueField("n_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN"),
                new SchemaAndValueField("nzs_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN"),
                new SchemaAndValueField("nvs_nan", Schema.OPTIONAL_STRING_SCHEMA, "NAN")));
        return fields;
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringEncodedNumericTypesWithInfinity() {
        return Arrays.asList(
                new SchemaAndValueField("d", Schema.OPTIONAL_STRING_SCHEMA, "1.10"),
                new SchemaAndValueField("dzs", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("dvs", Schema.OPTIONAL_STRING_SCHEMA, "10.1111"),
                new SchemaAndValueField("n", Schema.OPTIONAL_STRING_SCHEMA, "22.2200"),
                new SchemaAndValueField("nzs", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("nvs", Schema.OPTIONAL_STRING_SCHEMA, "22.2222"),
                new SchemaAndValueField("d_int", Schema.OPTIONAL_STRING_SCHEMA, "1.00"),
                new SchemaAndValueField("dzs_int", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("dvs_int", Schema.OPTIONAL_STRING_SCHEMA, "10"),
                new SchemaAndValueField("n_int", Schema.OPTIONAL_STRING_SCHEMA, "22.0000"),
                new SchemaAndValueField("nzs_int", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("nvs_int", Schema.OPTIONAL_STRING_SCHEMA, "22"),
                new SchemaAndValueField("dvs_nan", Schema.OPTIONAL_STRING_SCHEMA, "POSITIVE_INFINITY"),
                new SchemaAndValueField("nvs_nan", Schema.OPTIONAL_STRING_SCHEMA, "NEGATIVE_INFINITY"));
    }

    protected List<SchemaAndValueField> schemasAndValuesForDoubleEncodedNumericTypes() {
        final List<SchemaAndValueField> fields = new ArrayList<SchemaAndValueField>(Arrays.asList(
                new SchemaAndValueField("d", Schema.OPTIONAL_FLOAT64_SCHEMA, 1.1d),
                new SchemaAndValueField("dzs", Schema.OPTIONAL_FLOAT64_SCHEMA, 10d),
                new SchemaAndValueField("dvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 10.1111d),
                new SchemaAndValueField("n", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.22d),
                new SchemaAndValueField("nzs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22d),
                new SchemaAndValueField("nvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.2222d),
                new SchemaAndValueField("d_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("dzs_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("dvs_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("n_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("nzs_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN),
                new SchemaAndValueField("nvs_nan", Schema.OPTIONAL_FLOAT64_SCHEMA, Double.NaN)));
        return fields;
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForMapEncodedHStoreType() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("key", "val");
        return Arrays.asList(new SchemaAndValueField("hs", hstoreMapSchema(), expected));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForMapEncodedHStoreTypeWithMultipleValues() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "val1");
        expected.put("key2", "val2");
        expected.put("key3", "val3");

        Map<String, String> expectedArray1 = new HashMap<>();
        expectedArray1.put("key4", "val4");
        expectedArray1.put("key5", null);

        Map<String, String> expectedArray2 = new HashMap<>();
        expectedArray2.put("key6", "val6");

        return Arrays.asList(
                new SchemaAndValueField("hs", hstoreMapSchema(), expected),
                new SchemaAndValueField("hsarr", SchemaBuilder.array(hstoreMapSchema()).optional().build(), Arrays.asList(expectedArray1, expectedArray2)));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForMapEncodedHStoreTypeWithNullValues() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "val1");
        expected.put("key2", null);
        return Arrays.asList(new SchemaAndValueField("hs", hstoreMapSchema(), expected));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForMapEncodedHStoreTypeWithSpecialCharacters() {
        final Map<String, String> expected = new HashMap<>();
        expected.put("key_#1", "val 1");
        expected.put("key 2", " ##123 78");
        return Arrays.asList(new SchemaAndValueField("hs", hstoreMapSchema(), expected));
    }

    private Schema hstoreMapSchema() {
        return SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                SchemaBuilder.string().optional().build())
                .optional()
                .build();
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForJsonEncodedHStoreType() {
        final String expected = "{\"key\":\"val\"}";
        return Arrays.asList(new SchemaAndValueField("hs", Json.builder().optional().build(), expected));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForJsonEncodedHStoreTypeWithMultipleValues() {
        final String expected = "{\"key1\":\"val1\",\"key2\":\"val2\",\"key3\":\"val3\"}";
        final List<String> expectedArray = Arrays.asList(
                "{\"key5\":null,\"key4\":\"val4\"}",
                "{\"key6\":\"val6\"}");

        return Arrays.asList(
                new SchemaAndValueField("hs", Json.builder().optional().build(), expected),
                new SchemaAndValueField("hsarr", SchemaBuilder.array(Json.builder().optional().build()).optional().build(), expectedArray));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForJsonEncodedHStoreTypeWithNullValues() {
        final String expected = "{\"key1\":\"val1\",\"key2\":null}";
        return Arrays.asList(new SchemaAndValueField("hs", Json.builder().optional().build(), expected));
    }

    protected List<SchemaAndValueField> schemaAndValueFieldForJsonEncodedHStoreTypeWithSpcialCharacters() {
        final String expected = "{\"key_#1\":\"val 1\",\"key 2\":\" ##123 78\"}";
        return Arrays.asList(new SchemaAndValueField("hs", Json.builder().optional().build(), expected));
    }

    protected List<SchemaAndValueField> schemaAndValueForMacaddr8Type() {
        final String expected = "08:00:2b:01:02:03:04:05";
        return Arrays.asList(new SchemaAndValueField("m", Schema.OPTIONAL_STRING_SCHEMA, expected));
    }

    protected List<SchemaAndValueField> schemaAndValueForMaterializedViewBaseType() {
        return Arrays.asList(new SchemaAndValueField("i", Schema.OPTIONAL_INT32_SCHEMA, 1),
                new SchemaAndValueField("s", Schema.OPTIONAL_STRING_SCHEMA, "1"));
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypes() {
        return Arrays.asList(new SchemaAndValueField("vc", Schema.OPTIONAL_STRING_SCHEMA, "\u017E\u0161"),
                new SchemaAndValueField("vcv", Schema.OPTIONAL_STRING_SCHEMA, "bb"),
                new SchemaAndValueField("ch", Schema.OPTIONAL_STRING_SCHEMA, "cdef"),
                new SchemaAndValueField("c", Schema.OPTIONAL_STRING_SCHEMA, "abc"),
                new SchemaAndValueField("t", Schema.OPTIONAL_STRING_SCHEMA, "some text"),
                new SchemaAndValueField("b", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 0, 1, 2 })),
                new SchemaAndValueField("bnn", Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 3, 4, 5 })),
                new SchemaAndValueField("ct", Schema.OPTIONAL_STRING_SCHEMA, "Hello World"));
    }

    protected List<SchemaAndValueField> schemaAndValueForByteaBytes() {
        return Arrays.asList(
                new SchemaAndValueField("ba", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 1, 2, 3 })),
                new SchemaAndValueField("bytea_array", SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).optional().build(),
                        List.of(ByteBuffer.wrap(new byte[]{ 0, 1, 2 }), ByteBuffer.wrap(new byte[]{ 3, 4, 5 }))));
    }

    protected List<SchemaAndValueField> schemaAndValueForByteaHex() {
        return Arrays.asList(
                new SchemaAndValueField("ba", Schema.OPTIONAL_STRING_SCHEMA, "010203"),
                new SchemaAndValueField("bytea_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        List.of("000102", "030405")));
    }

    protected List<SchemaAndValueField> schemaAndValueForByteaBase64() {
        return Arrays.asList(
                new SchemaAndValueField("ba", Schema.OPTIONAL_STRING_SCHEMA, "AQID"),
                new SchemaAndValueField("bytea_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        List.of("AAEC", "AwQF")));
    }

    protected List<SchemaAndValueField> schemaAndValueForByteaBase64UrlSafe() {
        return Arrays.asList(
                new SchemaAndValueField("ba", Schema.OPTIONAL_STRING_SCHEMA, "AQID"),
                new SchemaAndValueField("bytea_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        List.of("AAEC", "AwQF")));
    }

    protected List<SchemaAndValueField> schemaAndValueForUnknownColumnBytes() {
        return Arrays.asList(new SchemaAndValueField("ccircle", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("<(10.0,20.0),10.0>".getBytes(StandardCharsets.UTF_8))));
    }

    protected List<SchemaAndValueField> schemaAndValueForUnknownColumnBase64() {
        return Arrays.asList(new SchemaAndValueField("ccircle", Schema.OPTIONAL_STRING_SCHEMA, "PCgxMC4wLDIwLjApLDEwLjA+"));
    }

    protected List<SchemaAndValueField> schemaAndValueForUnknownColumnBase64UrlSafe() {
        return Arrays.asList(new SchemaAndValueField("ccircle", Schema.OPTIONAL_STRING_SCHEMA, "PCgxMC4wLDIwLjApLDEwLjA-"));
    }

    protected List<SchemaAndValueField> schemaAndValueForUnknownColumnHex() {
        return Arrays.asList(new SchemaAndValueField("ccircle", Schema.OPTIONAL_STRING_SCHEMA, "3c2831302e302c32302e30292c31302e303e"));
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypesWithSourceColumnTypeInfo() {
        return Arrays.asList(new SchemaAndValueField("vc",
                SchemaBuilder.string().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "VARCHAR")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "2")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "vc")
                        .build(),
                "\u017E\u0161"),
                new SchemaAndValueField("vcv",
                        SchemaBuilder.string().optional()
                                .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "VARCHAR")
                                .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "2")
                                .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                                .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "vcv")
                                .build(),
                        "bb"),
                new SchemaAndValueField("ch", Schema.OPTIONAL_STRING_SCHEMA, "cdef"),
                new SchemaAndValueField("c", Schema.OPTIONAL_STRING_SCHEMA, "abc"),
                new SchemaAndValueField("t", Schema.OPTIONAL_STRING_SCHEMA, "some text"),
                new SchemaAndValueField("b", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 0, 1, 2 })),
                new SchemaAndValueField("bnn", Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 3, 4, 5 })));
    }

    protected List<SchemaAndValueField> schemasAndValuesForNetworkAddressTypes() {
        return Arrays.asList(new SchemaAndValueField("i", Schema.OPTIONAL_STRING_SCHEMA, "192.168.2.0/12"));
    }

    protected List<SchemaAndValueField> schemasAndValueForCidrAddressType() {
        return Arrays.asList(new SchemaAndValueField("i", Schema.OPTIONAL_STRING_SCHEMA, "192.168.100.128/25"));
    }

    protected List<SchemaAndValueField> schemasAndValueForMacaddrType() {
        return Arrays.asList(new SchemaAndValueField("m", Schema.OPTIONAL_STRING_SCHEMA, "08:00:2b:01:02:03"));
    }

    protected List<SchemaAndValueField> schemasAndValuesForNumericTypesWithSourceColumnTypeInfo() {
        return Arrays.asList(new SchemaAndValueField("d",
                SchemaBuilder.float64().optional()
                        .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "NUMERIC")
                        .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "3")
                        .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "2")
                        .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "d")
                        .build(),
                1.1d),
                new SchemaAndValueField("dzs",
                        SchemaBuilder.float64().optional()
                                .parameter(TestHelper.TYPE_NAME_PARAMETER_KEY, "NUMERIC")
                                .parameter(TestHelper.TYPE_LENGTH_PARAMETER_KEY, "4")
                                .parameter(TestHelper.TYPE_SCALE_PARAMETER_KEY, "0")
                                .parameter(TestHelper.COLUMN_NAME_PARAMETER_KEY, "dzs")
                                .build(),
                        10d),
                new SchemaAndValueField("dvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 10.1111d),
                new SchemaAndValueField("n", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.22d),
                new SchemaAndValueField("nzs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22d),
                new SchemaAndValueField("nvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.2222d));
    }

    protected List<SchemaAndValueField> schemasAndValuesForTextTypes() {
        return Arrays.asList(new SchemaAndValueField("j", Json.builder().optional().build(), "{\"bar\": \"baz\"}"),
                new SchemaAndValueField("jb", Json.builder().optional().build(), "{\"bar\": \"baz\"}"),
                new SchemaAndValueField("x", Xml.builder().optional().build(), "<foo>bar</foo><foo>bar</foo>"),
                new SchemaAndValueField("u", Uuid.builder().optional().build(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
    }

    protected List<SchemaAndValueField> schemaAndValuesForGeomTypes() {
        Schema pointSchema = Point.builder().optional().build();
        return Collections.singletonList(new SchemaAndValueField("p", pointSchema, Point.createValue(pointSchema, 1, 1)));
    }

    protected List<SchemaAndValueField> schemaAndValuesForRangeTypes() {
        String unboundedEnd = "infinity";

        // Tstrange type
        String beginTsrange = "2019-03-31 15:30:00";
        String endTsrange = "2019-04-30 15:30:00";

        String expectedUnboundedExclusiveTsrange = String.format("[\"%s\",%s)", beginTsrange, unboundedEnd);
        String expectedBoundedInclusiveTsrange = String.format("[\"%s\",\"%s\"]", beginTsrange, endTsrange);

        // Tstzrange type
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSx");
        Instant beginTstzrange = dateTimeFormatter.parse("2017-06-05 11:29:12.549426+00", Instant::from);
        Instant endTstzrange = dateTimeFormatter.parse("2017-06-05 12:34:56.789012+00", Instant::from);

        // Acknowledge timezone expectation of the system running the test
        String beginSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(beginTstzrange);
        String endSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(endTstzrange);

        String expectedUnboundedExclusiveTstzrange = String.format("[\"%s\",)", beginSystemTime);
        String expectedBoundedInclusiveTstzrange = String.format("[\"%s\",\"%s\"]", beginSystemTime, endSystemTime);

        // Daterange
        String beginDaterange = "2019-03-31";
        String endDaterange = "2019-04-30";

        String expectedUnboundedDaterange = String.format("[%s,%s)", beginDaterange, unboundedEnd);
        String expectedBoundedDaterange = String.format("[%s,%s)", beginDaterange, endDaterange);

        // int4range
        String beginrange = "1000";
        String endrange = "6000";

        String expectedrange = String.format("[%s,%s)", beginrange, endrange);

        // numrange
        String beginnumrange = "5.3";
        String endnumrange = "6.3";

        String expectednumrange = String.format("[%s,%s)", beginnumrange, endnumrange);

        // int8range
        String beginint8range = "1000000";
        String endint8range = "6000000";

        String expectedint8range = String.format("[%s,%s)", beginint8range, endint8range);

        return Arrays.asList(
                new SchemaAndValueField("unbounded_exclusive_tsrange", Schema.OPTIONAL_STRING_SCHEMA, expectedUnboundedExclusiveTsrange),
                new SchemaAndValueField("bounded_inclusive_tsrange", Schema.OPTIONAL_STRING_SCHEMA, expectedBoundedInclusiveTsrange),
                new SchemaAndValueField("unbounded_exclusive_tstzrange", Schema.OPTIONAL_STRING_SCHEMA, expectedUnboundedExclusiveTstzrange),
                new SchemaAndValueField("bounded_inclusive_tstzrange", Schema.OPTIONAL_STRING_SCHEMA, expectedBoundedInclusiveTstzrange),
                new SchemaAndValueField("unbounded_exclusive_daterange", Schema.OPTIONAL_STRING_SCHEMA, expectedUnboundedDaterange),
                new SchemaAndValueField("bounded_exclusive_daterange", Schema.OPTIONAL_STRING_SCHEMA, expectedBoundedDaterange),
                new SchemaAndValueField("int4_number_range", Schema.OPTIONAL_STRING_SCHEMA, expectedrange),
                new SchemaAndValueField("numerange", Schema.OPTIONAL_STRING_SCHEMA, expectednumrange),
                new SchemaAndValueField("int8_number_range", Schema.OPTIONAL_STRING_SCHEMA, expectedint8range));
    }

    protected List<SchemaAndValueField> schemaAndValuesForBinTypes() {
        return Arrays.asList(new SchemaAndValueField("ba", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 1, 2, 3 })),
                new SchemaAndValueField("bol", Schema.OPTIONAL_BOOLEAN_SCHEMA, false),
                new SchemaAndValueField("bol2", Schema.OPTIONAL_BOOLEAN_SCHEMA, true),
                new SchemaAndValueField("bs", Bits.builder(2).optional().build(), new byte[]{ 3 }),
                new SchemaAndValueField("bs7", Bits.builder(7).optional().build(), new byte[]{ 64 }),
                new SchemaAndValueField("bv", Bits.builder(2).optional().build(), new byte[]{}),
                new SchemaAndValueField("bv2", Bits.builder(24).optional().build(), new byte[]{ 1, 2, 3 }),
                new SchemaAndValueField("bvl", Bits.builder(64).optional().build(), new byte[]{ 0, 0, 0, 0, 0, 0, 0, -128 }), // Long.MAX_VALUE + 1
                new SchemaAndValueField("bvunlimited1", Bits.builder(Integer.MAX_VALUE).optional().build(), new byte[]{ 5 }),
                new SchemaAndValueField("bvunlimited2", Bits.builder(Integer.MAX_VALUE).optional().build(), new byte[]{ 1, 2, 35, -38, 1 }));
    }

    private long asEpochMillis(String timestamp) {
        return LocalDateTime.parse(timestamp).atOffset(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    private long asEpochMicros(String timestamp) {
        Instant instant = LocalDateTime.parse(timestamp).atOffset(ZoneOffset.UTC).toInstant();
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }

    protected List<SchemaAndValueField> schemaAndValuesForDateTimeTypes() {
        long expectedTs = asEpochMicros("2016-11-04T13:51:30.123456");
        long expectedTsMs = asEpochMillis("2016-11-04T13:51:30.123456");
        long expectedNegTs = asEpochMicros("1936-10-25T22:10:12.608");
        String expectedTz = "2016-11-04T11:51:30.123456Z"; // timestamp is stored with TZ, should be read back with UTC
        int expectedDate = Date.toEpochDay(LocalDate.parse("2016-11-04"), null);
        long expectedTi = LocalTime.parse("13:51:30").toNanoOfDay() / 1_000;
        long expectedTiPrecision = LocalTime.parse("13:51:30.123").toNanoOfDay() / 1_000_000;
        long expectedTtf = TimeUnit.DAYS.toNanos(1) / 1_000;
        String expectedTtz = "11:51:30.123789Z"; // time is stored with TZ, should be read back at GMT
        String expectedTtzPrecision = "11:51:30.123Z";
        long expectedInterval = MicroDuration.durationMicros(1, 2, 3, 4, 5, 6, 780000, MicroDuration.DAYS_PER_MONTH_AVG);

        long expectedTsLarge = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000 + 123456;
        long expectedTsLargeUs = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000 + 123457;
        long expectedTsLargeMs = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 124000000, ZoneOffset.UTC).toInstant().toEpochMilli();

        String expectedTzLarge = "+21016-11-04T06:51:30.123456Z";
        String expectedTzLargeZero = "+21016-11-04T06:51:30.000000Z";

        // The assertion for minimimum timestamps is problematic as it seems that Java and PostgreSQL handles conversion from large negative date
        // to microseconds in different way
        // written to database: 4713-12-31T23:59:59.999999 BC
        // arrived from decoding plugin: -210831897600000001
        // value from decoding plugin converted to Instant: -4712-12-31T23:59:59.999999Z - 1 year difference
        // JDBC driver delivers Timestamp B.C.E. 4713-12-31T23:59:59.000+0100 that internally contains -210835184391000001 and translates to Instant -4712-11-23T22:59:59.999999Z
        // The result is that JDBC driver and logical decoding plugin provides different values which moreover
        // do not map to Java conversions
        // It seems that JDBC driver always uses Gregorian calendar while Java Time API uses Julian
        // Also it seems that further distortions could be introduced by timezone used so instead of matching
        // against exact value the requested date should be smaller than -4712-11-01
        final SchemaAndValueField.Condition largeNegativeTimestamp = (String fieldName, Object expectedValue, Object actualValue) -> {
            final long expectedMinTsStreaming = -210831897600000001L;
            final long expectedMinTsSnapshot = LocalDateTime.of(-4712, 11, 1, 0, 0, 0).toInstant(java.time.ZoneOffset.UTC).getEpochSecond() * 1_000_000;
            final long ts = (long) actualValue;
            assertTrue("Negative timestamp don't match for " + fieldName + ", got " + actualValue, ts >= expectedMinTsSnapshot && ts < 0);
        };
        // The same issue is with timezoned timestamp
        // Database: 4714-12-31T23:59:59.999999Z BC
        // JDBC: -4713-11-23T23:59:59.999999Z
        // Streamed as: -210863520000000001
        // Java conversion: -4713-12-31T23:59:59.999999Z
        final SchemaAndValueField.Condition largeNegativeTzTimestamp = (String fieldName, Object expectedValue, Object actualValue) -> {
            final String expectedMinTsStreaming = "-4713-12-31T23:59:59.999999Z";
            final String expectedMinTsSnapshot = "-4713-11-23T23:59:59.999999Z";
            assertTrue("Negative timestamp don't match for " + fieldName + ", got " + actualValue,
                    expectedMinTsSnapshot.equals(actualValue) || expectedMinTsStreaming.equals(actualValue));
        };
        return Arrays.asList(new SchemaAndValueField("ts", MicroTimestamp.builder().optional().build(), expectedTs),
                new SchemaAndValueField("tsneg", MicroTimestamp.builder().optional().build(), expectedNegTs),
                new SchemaAndValueField("ts_ms", Timestamp.builder().optional().build(), expectedTsMs),
                new SchemaAndValueField("ts_us", MicroTimestamp.builder().optional().build(), expectedTs),
                new SchemaAndValueField("tz", ZonedTimestamp.builder().optional().build(), expectedTz),
                new SchemaAndValueField("date", Date.builder().optional().build(), expectedDate),
                new SchemaAndValueField("ti", MicroTime.builder().optional().build(), expectedTi),
                new SchemaAndValueField("tip", Time.builder().optional().build(), (int) expectedTiPrecision),
                new SchemaAndValueField("ttf", MicroTime.builder().optional().build(), expectedTtf),
                new SchemaAndValueField("ttz", ZonedTime.builder().optional().build(), expectedTtz),
                new SchemaAndValueField("tptz", ZonedTime.builder().optional().build(), expectedTtzPrecision),
                new SchemaAndValueField("it", MicroDuration.builder().optional().build(), expectedInterval),
                new SchemaAndValueField("ts_large", MicroTimestamp.builder().optional().build(), expectedTsLarge),
                new SchemaAndValueField("ts_large_us", MicroTimestamp.builder().optional().build(), expectedTsLargeUs),
                new SchemaAndValueField("ts_large_ms", Timestamp.builder().optional().build(), expectedTsLargeMs),
                new SchemaAndValueField("tz_large", ZonedTimestamp.builder().optional().build(), expectedTzLarge),
                new SchemaAndValueField("ts_max", MicroTimestamp.builder().optional().build(), 9223371331200000000L - 1L),
                new SchemaAndValueField("ts_min", MicroTimestamp.builder().optional().build(), -1L).assertWithCondition(largeNegativeTimestamp),
                new SchemaAndValueField("tz_max", ZonedTimestamp.builder().optional().build(), "+294247-01-01T23:59:59.999999Z"),
                new SchemaAndValueField("tz_min", ZonedTimestamp.builder().optional().build(), "").assertWithCondition(largeNegativeTzTimestamp),
                new SchemaAndValueField("ts_pinf", MicroTimestamp.builder().optional().build(), PgStatement.DATE_POSITIVE_INFINITY),
                new SchemaAndValueField("ts_ninf", MicroTimestamp.builder().optional().build(), PgStatement.DATE_NEGATIVE_INFINITY),
                new SchemaAndValueField("tz_pinf", ZonedTimestamp.builder().optional().build(), "infinity"),
                new SchemaAndValueField("tz_ninf", ZonedTimestamp.builder().optional().build(), "-infinity"),
                new SchemaAndValueField("tz_zero", ZonedTimestamp.builder().optional().build(), expectedTzLargeZero));
    }

    protected List<SchemaAndValueField> schemaAndValuesForTimeArrayTypes() {
        final long expectedTime1 = LocalTime.parse("00:01:02").toNanoOfDay() / 1_000;
        final long expectedTime2 = LocalTime.parse("01:02:03").toNanoOfDay() / 1_000;
        final String expectedTimeTz1 = "11:51:02Z";
        final String expectedTimeTz2 = "12:51:03Z";
        final long expectedTimestamp1 = OffsetDateTime.of(2020, 4, 1, 0, 1, 2, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
        final long expectedTimestamp2 = OffsetDateTime.of(2020, 4, 1, 1, 2, 3, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
        final String expectedTimestampTz1 = "2020-04-01T11:51:02.000000Z";
        final String expectedTimestampTz2 = "2020-04-01T12:51:03.000000Z";

        return Arrays.asList(new SchemaAndValueField("timea",
                SchemaBuilder.array(MicroTime.builder().optional().build()).build(),
                Arrays.asList(expectedTime1, expectedTime2)),
                new SchemaAndValueField("timetza",
                        SchemaBuilder.array(ZonedTime.builder().optional().build()).build(),
                        Arrays.asList(expectedTimeTz1, expectedTimeTz2)),
                new SchemaAndValueField("timestampa",
                        SchemaBuilder.array(MicroTimestamp.builder().optional().build()).build(),
                        Arrays.asList(expectedTimestamp1, expectedTimestamp2)),
                new SchemaAndValueField("timestamptza",
                        SchemaBuilder.array(ZonedTimestamp.builder().optional().build()).build(),
                        Arrays.asList(expectedTimestampTz1, expectedTimestampTz2)));
    }

    protected List<SchemaAndValueField> schemaAndValuesForIntervalAsString() {
        // 1 year, 2 months, 3 days, 4 hours, 5 minutes, 6 seconds, 78000 ms
        final String expectedInterval = "P1Y2M3DT4H5M6.78S";
        return Arrays.asList(
                new SchemaAndValueField("it", Interval.builder().optional().build(), expectedInterval));
    }

    protected List<SchemaAndValueField> schemaAndValuesForDateTimeTypesAdaptiveTimeMicroseconds() {
        long expectedTs = asEpochMicros("2016-11-04T13:51:30.123456");
        long expectedTsMs = asEpochMillis("2016-11-04T13:51:30.123456");
        long expectedNegTs = asEpochMicros("1936-10-25T22:10:12.608");
        String expectedTz = "2016-11-04T11:51:30.123456Z"; // timestamp is stored with TZ, should be read back with UTC
        int expectedDate = Date.toEpochDay(LocalDate.parse("2016-11-04"), null);
        long expectedTi = LocalTime.parse("13:51:30").toNanoOfDay() / 1_000;
        String expectedTtz = "11:51:30.123789Z"; // time is stored with TZ, should be read back at GMT
        long expectedInterval = MicroDuration.durationMicros(1, 2, 3, 4, 5, 6.78, MicroDuration.DAYS_PER_MONTH_AVG);

        long expectedTsLarge = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000 + 123456;
        long expectedTsLargeUs = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 0, ZoneOffset.UTC).toInstant().toEpochMilli() * 1000 + 123457;
        long expectedTsLargeMs = OffsetDateTime.of(21016, 11, 4, 13, 51, 30, 124000000, ZoneOffset.UTC).toInstant().toEpochMilli();

        return Arrays.asList(new SchemaAndValueField("ts", MicroTimestamp.builder().optional().build(), expectedTs),
                new SchemaAndValueField("tsneg", MicroTimestamp.builder().optional().build(), expectedNegTs),
                new SchemaAndValueField("ts_ms", Timestamp.builder().optional().build(), expectedTsMs),
                new SchemaAndValueField("ts_us", MicroTimestamp.builder().optional().build(), expectedTs),
                new SchemaAndValueField("tz", ZonedTimestamp.builder().optional().build(), expectedTz),
                new SchemaAndValueField("date", Date.builder().optional().build(), expectedDate),
                new SchemaAndValueField("ti", MicroTime.builder().optional().build(), expectedTi),
                new SchemaAndValueField("ttz", ZonedTime.builder().optional().build(), expectedTtz),
                new SchemaAndValueField("it", MicroDuration.builder().optional().build(), expectedInterval),
                new SchemaAndValueField("ts_large", MicroTimestamp.builder().optional().build(), expectedTsLarge),
                new SchemaAndValueField("ts_large_us", MicroTimestamp.builder().optional().build(), expectedTsLargeUs),
                new SchemaAndValueField("ts_large_ms", Timestamp.builder().optional().build(), expectedTsLargeMs));
    }

    protected List<SchemaAndValueField> schemaAndValuesForMoneyTypes() {
        return Collections.singletonList(new SchemaAndValueField("csh", Decimal.builder(2).optional().build(),
                BigDecimal.valueOf(1234.11d)));
    }

    protected List<SchemaAndValueField> schemaAndValuesForNegativeMoneyTypes() {
        return Collections.singletonList(new SchemaAndValueField("csh", Decimal.builder(2).optional().build(),
                BigDecimal.valueOf(-1234.11d)));
    }

    protected List<SchemaAndValueField> schemaAndValuesForNullMoneyTypes() {
        return Collections.singletonList(new SchemaAndValueField("csh", Decimal.builder(2).optional().build(), null));
    }

    protected List<SchemaAndValueField> schemasAndValuesForArrayTypes() {
        Struct element;
        final List<Struct> varnumArray = new ArrayList<>();
        element = new Struct(VariableScaleDecimal.schema());
        element.put("scale", 1).put("value", new BigDecimal("1.1").unscaledValue().toByteArray());
        varnumArray.add(element);
        element = new Struct(VariableScaleDecimal.schema());
        element.put("scale", 2).put("value", new BigDecimal("2.22").unscaledValue().toByteArray());
        varnumArray.add(element);
        element = new Struct(VariableScaleDecimal.schema());
        element.put("scale", 3).put("value", new BigDecimal("3.333").unscaledValue().toByteArray());
        varnumArray.add(element);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSx");
        Instant begin = dateTimeFormatter.parse("2017-06-05 11:29:12.549426+00", Instant::from);
        Instant end = dateTimeFormatter.parse("2017-06-05 12:34:56.789012+00", Instant::from);

        // Acknowledge timezone expectation of the system running the test
        String beginSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(begin);
        String endSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(end);

        String expectedFirstTstzrange = String.format("[\"%s\",)", beginSystemTime);
        String expectedSecondTstzrange = String.format("[\"%s\",\"%s\"]", beginSystemTime, endSystemTime);

        return Arrays.asList(new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
                Arrays.asList(1, 2, 3)),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
                        Arrays.asList(1550166368505037572L)),
                new SchemaAndValueField("text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("one", "two", "three")),
                new SchemaAndValueField("char_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("cone      ", "ctwo      ", "cthree    ")),
                new SchemaAndValueField("varchar_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("vcone", "vctwo", "vcthree")),
                new SchemaAndValueField("date_array", SchemaBuilder.array(Date.builder().optional().schema()).optional().build(),
                        Arrays.asList(
                                (int) LocalDate.of(2016, Month.NOVEMBER, 4).toEpochDay(),
                                (int) LocalDate.of(2016, Month.NOVEMBER, 5).toEpochDay(),
                                (int) LocalDate.of(2016, Month.NOVEMBER, 6).toEpochDay())),
                new SchemaAndValueField("numeric_array",
                        SchemaBuilder.array(Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "10").optional().build()).optional().build(),
                        Arrays.asList(
                                new BigDecimal("1.20"),
                                new BigDecimal("3.40"),
                                new BigDecimal("5.60"))),
                new SchemaAndValueField("varnumeric_array", SchemaBuilder.array(VariableScaleDecimal.builder().optional().build()).optional().build(),
                        varnumArray),
                new SchemaAndValueField("citext_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("four", "five", "six")),
                new SchemaAndValueField("inet_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("192.168.2.0/12", "192.168.1.1", "192.168.0.2/1")),
                new SchemaAndValueField("cidr_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("192.168.100.128/25", "192.168.0.0/25", "192.168.1.0/24")),
                new SchemaAndValueField("macaddr_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("08:00:2b:01:02:03", "08:00:2b:01:02:03", "08:00:2b:01:02:03")),
                new SchemaAndValueField("tsrange_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("[\"2019-03-31 15:30:00\",infinity)", "[\"2019-03-31 15:30:00\",\"2019-04-30 15:30:00\"]")),
                new SchemaAndValueField("tstzrange_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList(expectedFirstTstzrange, expectedSecondTstzrange)),
                new SchemaAndValueField("daterange_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("[2019-03-31,infinity)", "[2019-03-31,2019-04-30)")),
                new SchemaAndValueField("int4range_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("[1,6)", "[1,4)")),
                new SchemaAndValueField("numerange_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("[5.3,6.3)", "[10.0,20.0)")),
                new SchemaAndValueField("int8range_array", SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA).optional().build(),
                        Arrays.asList("[1000000,6000000)", "[5000,9000)")),
                new SchemaAndValueField("uuid_array", SchemaBuilder.array(Uuid.builder().optional().build()).optional().build(),
                        Arrays.asList("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "f0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")),
                new SchemaAndValueField("json_array", SchemaBuilder.array(Json.builder().optional().build()).optional().build(),
                        Arrays.asList("{\"bar\": \"baz\"}", "{\"foo\": \"qux\"}")),
                new SchemaAndValueField("jsonb_array", SchemaBuilder.array(Json.builder().optional().build()).optional().build(),
                        Arrays.asList("{\"bar\": \"baz\"}", "{\"foo\": \"qux\"}")),
                new SchemaAndValueField("oid_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
                        Arrays.asList(3L, 4_000_000_000L)));
    }

    protected List<SchemaAndValueField> schemasAndValuesForArrayTypesWithNullValues() {
        return Arrays.asList(
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), null),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), null),
                new SchemaAndValueField("text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("char_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("varchar_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("date_array", SchemaBuilder.array(Date.builder().optional().schema()).optional().build(), null),
                new SchemaAndValueField("numeric_array",
                        SchemaBuilder.array(Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "10").optional().build()).optional().build(), null),
                new SchemaAndValueField("citext_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("inet_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("cidr_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("macaddr_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("tsrange_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("tstzrange_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("daterange_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("int4range_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("numerange_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("int8range_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("uuid_array", SchemaBuilder.array(Uuid.builder().optional().build()).optional().build(), null));
    }

    protected List<SchemaAndValueField> schemaAndValuesForPostgisTypes() {
        Schema geomSchema = Geometry.builder().optional().build();
        Schema geogSchema = Geography.builder().optional().build();
        return Arrays.asList(
                // geometries are encoded here as HexEWKB
                new SchemaAndValueField("p", geomSchema,
                        // 'SRID=3187;POINT(174.9479 -36.7208)'::postgis.geometry
                        Geometry.createValue(geomSchema, DatatypeConverter.parseHexBinary("0101000020730C00001C7C613255DE6540787AA52C435C42C0"), 3187)),
                new SchemaAndValueField("ml", geogSchema,
                        // 'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::postgis.geography
                        Geography.createValue(geogSchema, DatatypeConverter
                                .parseHexBinary("0105000020E610000001000000010200000002000000A779C7293A2465400B462575025A46C0C66D3480B7FC6440C3D32B65195246C0"), 4326)));
    }

    protected List<SchemaAndValueField> schemaAndValuesForPostgisArrayTypes() {
        Schema geomSchema = Geometry.builder()
                .optional()
                .build();

        List<Struct> values = Arrays.asList(
                // 'GEOMETRYCOLLECTION EMPTY'::postgis.geometry
                Geometry.createValue(geomSchema, DatatypeConverter.parseHexBinary("010700000000000000"), null),
                // 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::postgis.geometry
                Geometry.createValue(geomSchema, DatatypeConverter.parseHexBinary(
                        "01030000000100000005000000B81E85EB51D0644052B81E85EB5147C0713D0AD7A350664052B81E85EB5147C0713D0AD7A35066409A999999993941C0B81E85EB51D064409A999999993941C0B81E85EB51D0644052B81E85EB5147C0"),
                        null));
        return Arrays.asList(
                // geometries are encoded here as HexEWKB
                new SchemaAndValueField("ga", SchemaBuilder.array(geomSchema).optional().build(), values),
                new SchemaAndValueField("gann", SchemaBuilder.array(geomSchema).build(), values));
    }

    protected List<SchemaAndValueField> schemasAndValuesForQuotedTypes() {
        return Arrays.asList(new SchemaAndValueField("Quoted_\" . Text_Column", Schema.OPTIONAL_STRING_SCHEMA, "some text"));
    }

    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTopicName() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::topicNameFromInsertStmt,
                this::schemasAndValuesForTable));
    }

    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTopicNameAdaptiveTimeMicroseconds() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::topicNameFromInsertStmt,
                this::schemasAndValuesForTableAdaptiveTimeMicroseconds));
    }

    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTopicNameStringEncodedDecimals() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::topicNameFromInsertStmt,
                this::schemasAndValuesForNumericTypesUsingStringEncoding));
    }

    protected List<SchemaAndValueField> schemasAndValuesForTableAdaptiveTimeMicroseconds(String insertTableStatement) {
        if (insertTableStatement.equals(INSERT_DATE_TIME_TYPES_STMT)) {
            return schemaAndValuesForDateTimeTypesAdaptiveTimeMicroseconds();
        }
        return schemasAndValuesForTable(insertTableStatement);
    }

    protected List<SchemaAndValueField> schemasAndValuesForNumericTypesUsingStringEncoding(String insertTableStatement) {
        if (insertTableStatement.equals(INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN)) {
            return schemasAndValuesForStringEncodedNumericTypes();
        }
        return schemasAndValuesForTable(insertTableStatement);
    }

    protected List<SchemaAndValueField> schemasAndValuesForCustomTypes() {
        final Schema ltreeSchema = Ltree.builder().optional().build();
        final Schema ltreeArraySchema = SchemaBuilder.array(ltreeSchema).optional().build();
        return Arrays.asList(new SchemaAndValueField("lt", ltreeSchema, "Top.Collections.Pictures.Astronomy.Galaxies"),
                new SchemaAndValueField("i", Schema.STRING_SCHEMA, "0-393-04002-X"),
                new SchemaAndValueField("n", Schema.OPTIONAL_STRING_SCHEMA, null),
                new SchemaAndValueField("lt_array", ltreeArraySchema, Arrays.asList("Ship.Frigate", "Ship.Destroyer")));
    }

    protected List<SchemaAndValueField> schemasAndValuesForCustomConverterTypes() {
        final Schema ltreeSchema = Ltree.builder().optional().build();
        final Schema ltreeArraySchema = SchemaBuilder.array(ltreeSchema).optional().build();
        return Arrays.asList(new SchemaAndValueField("lt", ltreeSchema, "Top.Collections.Pictures.Astronomy.Galaxies"),
                new SchemaAndValueField("i", SchemaBuilder.string().name("io.debezium.postgresql.type.Isbn").build(), "0-393-04002-X"),
                new SchemaAndValueField("n", Schema.OPTIONAL_STRING_SCHEMA, null),
                new SchemaAndValueField("lt_array", ltreeArraySchema, Arrays.asList("Ship.Frigate", "Ship.Destroyer")));
    }

    protected List<SchemaAndValueField> schemasAndValuesForDomainAliasTypes(boolean streaming) {
        final ByteBuffer boxByteBuffer = ByteBuffer.wrap("(1.0,1.0),(0.0,0.0)".getBytes());
        final ByteBuffer circleByteBuffer = ByteBuffer.wrap("<(10.0,4.0),10.0>".getBytes());
        final ByteBuffer lineByteBuffer = ByteBuffer.wrap("{-1.0,0.0,0.0}".getBytes());
        final ByteBuffer lsegByteBuffer = ByteBuffer.wrap("[(0.0,0.0),(0.0,1.0)]".getBytes());
        final ByteBuffer pathByteBuffer = ByteBuffer.wrap("((0.0,0.0),(0.0,1.0),(0.0,2.0))".getBytes());
        final ByteBuffer polygonByteBuffer = ByteBuffer.wrap("((0.0,0.0),(0.0,1.0),(1.0,0.0),(0.0,0.0))".getBytes());

        return Arrays.asList(
                new SchemaAndValueField(PK_FIELD, SchemaBuilder.int32().defaultValue(0).build(), 1),
                new SchemaAndValueField("bit_base", Bits.builder(3).build(), new byte[]{ 5 }),
                new SchemaAndValueField("bit_alias", Bits.builder(3).build(), new byte[]{ 5 }),
                new SchemaAndValueField("smallint_base", SchemaBuilder.INT16_SCHEMA, (short) 1),
                new SchemaAndValueField("smallint_alias", SchemaBuilder.INT16_SCHEMA, (short) 1),
                new SchemaAndValueField("integer_base", SchemaBuilder.INT32_SCHEMA, 1),
                new SchemaAndValueField("integer_alias", SchemaBuilder.INT32_SCHEMA, 1),
                new SchemaAndValueField("bigint_base", SchemaBuilder.INT64_SCHEMA, 1000L),
                new SchemaAndValueField("bigint_alias", SchemaBuilder.INT64_SCHEMA, 1000L),
                new SchemaAndValueField("real_base", SchemaBuilder.FLOAT32_SCHEMA, 3.14f),
                new SchemaAndValueField("real_alias", SchemaBuilder.FLOAT32_SCHEMA, 3.14f),
                new SchemaAndValueField("float8_base", SchemaBuilder.FLOAT64_SCHEMA, 3.14),
                new SchemaAndValueField("float8_alias", SchemaBuilder.FLOAT64_SCHEMA, 3.14),
                new SchemaAndValueField("numeric_base", SpecialValueDecimal.builder(DecimalMode.DOUBLE, 4, 2).build(), 1234.12),
                new SchemaAndValueField("numeric_alias", SpecialValueDecimal.builder(DecimalMode.DOUBLE, 4, 2).build(), 1234.12),
                new SchemaAndValueField("bool_base", SchemaBuilder.BOOLEAN_SCHEMA, true),
                new SchemaAndValueField("bool_alias", SchemaBuilder.BOOLEAN_SCHEMA, true),
                new SchemaAndValueField("string_base", SchemaBuilder.STRING_SCHEMA, "hello"),
                new SchemaAndValueField("string_alias", SchemaBuilder.STRING_SCHEMA, "hello"),
                new SchemaAndValueField("date_base", Date.builder().build(), Date.toEpochDay(LocalDate.parse("2019-10-02"), null)),
                new SchemaAndValueField("date_alias", Date.builder().build(), Date.toEpochDay(LocalDate.parse("2019-10-02"), null)),
                new SchemaAndValueField("time_base", MicroTime.builder().build(), LocalTime.parse("01:02:03").toNanoOfDay() / 1_000),
                new SchemaAndValueField("time_alias", MicroTime.builder().build(), LocalTime.parse("01:02:03").toNanoOfDay() / 1_000),
                new SchemaAndValueField("timetz_base", ZonedTime.builder().build(), "01:02:03.123789Z"),
                new SchemaAndValueField("timetz_alias", ZonedTime.builder().build(), "01:02:03.123789Z"),
                new SchemaAndValueField("timestamp_base", MicroTimestamp.builder().build(), asEpochMicros("2019-10-02T01:02:03.123456")),
                new SchemaAndValueField("timestamp_alias", MicroTimestamp.builder().build(), asEpochMicros("2019-10-02T01:02:03.123456")),
                new SchemaAndValueField("timestamptz_base", ZonedTimestamp.builder().build(), "2019-10-02T11:51:30.123456Z"),
                new SchemaAndValueField("timestamptz_alias", ZonedTimestamp.builder().build(), "2019-10-02T11:51:30.123456Z"),
                new SchemaAndValueField("timewottz_base", MicroTime.builder().build(), LocalTime.parse("01:02:03").toNanoOfDay() / 1_000),
                new SchemaAndValueField("timewottz_alias", MicroTime.builder().build(), LocalTime.parse("01:02:03").toNanoOfDay() / 1_000),
                new SchemaAndValueField("interval_base", MicroDuration.builder().build(),
                        MicroDuration.durationMicros(1, 2, 3, 4, 5, 6, MicroDuration.DAYS_PER_MONTH_AVG)),
                new SchemaAndValueField("interval_alias", MicroDuration.builder().build(),
                        MicroDuration.durationMicros(1, 2, 3, 4, 5, 6, MicroDuration.DAYS_PER_MONTH_AVG)),
                new SchemaAndValueField("box_base", SchemaBuilder.BYTES_SCHEMA, boxByteBuffer),
                new SchemaAndValueField("box_alias", SchemaBuilder.BYTES_SCHEMA, boxByteBuffer),
                new SchemaAndValueField("circle_base", SchemaBuilder.BYTES_SCHEMA, circleByteBuffer),
                new SchemaAndValueField("circle_alias", SchemaBuilder.BYTES_SCHEMA, circleByteBuffer),
                new SchemaAndValueField("line_base", SchemaBuilder.BYTES_SCHEMA, lineByteBuffer),
                new SchemaAndValueField("line_alias", SchemaBuilder.BYTES_SCHEMA, lineByteBuffer),
                new SchemaAndValueField("lseg_base", SchemaBuilder.BYTES_SCHEMA, lsegByteBuffer),
                new SchemaAndValueField("lseg_alias", SchemaBuilder.BYTES_SCHEMA, lsegByteBuffer),
                new SchemaAndValueField("path_base", SchemaBuilder.BYTES_SCHEMA, pathByteBuffer),
                new SchemaAndValueField("path_alias", SchemaBuilder.BYTES_SCHEMA, pathByteBuffer),
                new SchemaAndValueField("point_base", Point.builder().build(), Point.createValue(Point.builder().build(), 1, 1)),
                new SchemaAndValueField("point_alias", Point.builder().build(), Point.createValue(Point.builder().build(), 1, 1)),
                new SchemaAndValueField("polygon_base", SchemaBuilder.BYTES_SCHEMA, polygonByteBuffer),
                new SchemaAndValueField("polygon_alias", SchemaBuilder.BYTES_SCHEMA, polygonByteBuffer),
                new SchemaAndValueField("char_base", SchemaBuilder.STRING_SCHEMA, "a"),
                new SchemaAndValueField("char_alias", SchemaBuilder.STRING_SCHEMA, "a"),
                new SchemaAndValueField("text_base", SchemaBuilder.STRING_SCHEMA, "Hello World"),
                new SchemaAndValueField("text_alias", SchemaBuilder.STRING_SCHEMA, "Hello World"),
                new SchemaAndValueField("json_base", Json.builder().build(), "{\"key\": \"value\"}"),
                new SchemaAndValueField("json_alias", Json.builder().build(), "{\"key\": \"value\"}"),
                new SchemaAndValueField("xml_base", Xml.builder().build(), "<foo>Hello</foo>"),
                new SchemaAndValueField("xml_alias", Xml.builder().build(), "<foo>Hello</foo>"),
                new SchemaAndValueField("uuid_base", Uuid.builder().build(), "40e6215d-b5c6-4896-987c-f30f3678f608"),
                new SchemaAndValueField("uuid_alias", Uuid.builder().build(), "40e6215d-b5c6-4896-987c-f30f3678f608"),
                new SchemaAndValueField("varbit_base", Bits.builder(3).build(), new byte[]{ 5 }),
                new SchemaAndValueField("varbit_alias", Bits.builder(3).build(), new byte[]{ 5 }),
                new SchemaAndValueField("inet_base", SchemaBuilder.STRING_SCHEMA, "192.168.0.1"),
                new SchemaAndValueField("inet_alias", SchemaBuilder.STRING_SCHEMA, "192.168.0.1"),
                new SchemaAndValueField("cidr_base", SchemaBuilder.STRING_SCHEMA, "192.168.0.0/24"),
                new SchemaAndValueField("cidr_alias", SchemaBuilder.STRING_SCHEMA, "192.168.0.0/24"),
                new SchemaAndValueField("macaddr_base", SchemaBuilder.STRING_SCHEMA, "08:00:2b:01:02:03"),
                new SchemaAndValueField("macaddr_alias", SchemaBuilder.STRING_SCHEMA, "08:00:2b:01:02:03"));
    }

    protected List<SchemaAndValueField> schemasAndValuesForTable(String insertTableStatement) {
        switch (insertTableStatement) {
            case INSERT_NUMERIC_TYPES_STMT:
                return schemasAndValuesForNumericType();
            case INSERT_NUMERIC_DECIMAL_TYPES_STMT_NO_NAN:
                return schemasAndValuesForBigDecimalEncodedNumericTypes();
            case INSERT_BIN_TYPES_STMT:
                return schemaAndValuesForBinTypes();
            case INSERT_CASH_TYPES_STMT:
                return schemaAndValuesForMoneyTypes();
            case INSERT_DATE_TIME_TYPES_STMT:
                return schemaAndValuesForDateTimeTypes();
            case INSERT_GEOM_TYPES_STMT:
                return schemaAndValuesForGeomTypes();
            case INSERT_STRING_TYPES_STMT:
                return schemasAndValuesForStringTypes();
            case INSERT_NETWORK_ADDRESS_TYPES_STMT:
                return schemasAndValuesForNetworkAddressTypes();
            case INSERT_CIDR_NETWORK_ADDRESS_TYPE_STMT:
                return schemasAndValueForCidrAddressType();
            case INSERT_MACADDR_TYPE_STMT:
                return schemasAndValueForMacaddrType();
            case INSERT_RANGE_TYPES_STMT:
                return schemaAndValuesForRangeTypes();
            case INSERT_TEXT_TYPES_STMT:
                return schemasAndValuesForTextTypes();
            case INSERT_ARRAY_TYPES_STMT:
                return schemasAndValuesForArrayTypes();
            case INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT:
                return schemasAndValuesForArrayTypesWithNullValues();
            case INSERT_CUSTOM_TYPES_STMT:
                return schemasAndValuesForCustomTypes();
            case INSERT_POSTGIS_TYPES_STMT:
                return schemaAndValuesForPostgisTypes();
            case INSERT_POSTGIS_ARRAY_TYPES_STMT:
                return schemaAndValuesForPostgisArrayTypes();
            case INSERT_QUOTED_TYPES_STMT:
                return schemasAndValuesForQuotedTypes();
            default:
                throw new IllegalArgumentException("unknown statement:" + insertTableStatement);
        }
    }

    protected void assertRecordSchemaAndValues(List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                               SourceRecord record,
                                               String envelopeFieldName) {
        Struct content = ((Struct) record.value()).getStruct(envelopeFieldName);

        if (expectedSchemaAndValuesByColumn == null) {
            assertThat(content).isNull();
        }
        else {
            assertNotNull("expected there to be content in Envelope under " + envelopeFieldName, content);
            expectedSchemaAndValuesByColumn.forEach(
                    schemaAndValueField -> schemaAndValueField.assertFor(content));
        }
    }

    protected SnapshotRecord expectedSnapshotRecordFromPosition(int totalPosition, int totalCount, int topicPosition, int topicCount) {
        if (totalPosition == totalCount) {
            return SnapshotRecord.LAST;
        }

        if (totalPosition == 1) {
            return SnapshotRecord.FIRST;
        }

        if (topicPosition == topicCount) {
            return SnapshotRecord.LAST_IN_DATA_COLLECTION;
        }

        if (topicPosition == 1) {
            return SnapshotRecord.FIRST_IN_DATA_COLLECTION;
        }

        return SnapshotRecord.TRUE;
    }

    protected void assertRecordOffsetAndSnapshotSource(SourceRecord record, SnapshotRecord expectedType) {
        Map<String, ?> offset = record.sourceOffset();
        assertNotNull(offset.get(SourceInfo.TXID_KEY));
        assertNotNull(offset.get(SourceInfo.TIMESTAMP_USEC_KEY));
        assertNotNull(offset.get(SourceInfo.LSN_KEY));
        Object snapshot = offset.get(SourceInfo.SNAPSHOT_KEY);

        Object lastSnapshotRecord = offset.get(SourceInfo.LAST_SNAPSHOT_RECORD_KEY);

        if (expectedType != SnapshotRecord.FALSE) {
            assertEquals(SnapshotType.INITIAL.toString(), snapshot);
        }
        else {
            assertNull("Snapshot marker not expected, but found", snapshot);
            assertNull("Last snapshot marker not expected, but found", lastSnapshotRecord);
        }
        final Struct envelope = (Struct) record.value();
        if (envelope != null && Envelope.isEnvelopeSchema(envelope.schema())) {
            final Struct source = (Struct) envelope.get("source");
            final SnapshotRecord sourceSnapshot = SnapshotRecord.fromSource(source);
            if (sourceSnapshot != null) {
                assertEquals("Expected snapshot of type, but found", expectedType, sourceSnapshot);
            }
            else {
                assertEquals("Source snapshot marker not expected, but found", expectedType, SnapshotRecord.FALSE);
            }
        }
    }

    protected void assertSourceInfo(SourceRecord record, String db, String schema, String table) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        assertEquals(db, source.getString("db"));
        assertEquals(schema, source.getString("schema"));
        assertEquals(table, source.getString("table"));
    }

    protected void assertSourceInfo(SourceRecord record) {
        assertTrue(record.value() instanceof Struct);
        Struct source = ((Struct) record.value()).getStruct("source");
        assertNotNull(source.getString("db"));
        assertNotNull(source.getString("schema"));
        assertNotNull(source.getString("table"));
    }

    protected static String topicNameFromInsertStmt(String statement) {
        String qualifiedTableNameName = tableIdFromInsertStmt(statement).toString();
        return qualifiedTableNameName.replaceAll("[ \"]", "_");
    }

    protected static TableId tableIdFromInsertStmt(String statement) {
        Matcher matcher = INSERT_TABLE_MATCHING_PATTERN.matcher(statement);
        assertTrue("Extraction of table name from insert statement failed: " + statement, matcher.matches());

        TableId id = TableId.parse(matcher.group(1), false);

        if (id.schema() == null) {
            id = new TableId(id.catalog(), "public", id.table());
        }

        return id;
    }

    protected static TableId tableIdFromDeleteStmt(String statement) {
        Matcher matcher = DELETE_TABLE_MATCHING_PATTERN.matcher(statement);
        assertTrue("Extraction of table name from insert statement failed: " + statement, matcher.matches());

        TableId id = TableId.parse(matcher.group(1), false);

        if (id.schema() == null) {
            id = new TableId(id.catalog(), "public", id.table());
        }

        return id;
    }

    protected static class SchemaAndValueField {
        @FunctionalInterface
        protected interface Condition {
            void assertField(String fieldName, Object expectedValue, Object actualValue);
        }

        private final Schema schema;
        private final Object value;
        private final String fieldName;
        private Supplier<Boolean> assertValueOnlyIf = null;
        private Condition valueCondition = (fieldName, expectedValue, actualValue) -> {
            assertEquals("Values don't match for " + fieldName, expectedValue, actualValue);
        };

        public SchemaAndValueField(String fieldName, Schema schema, Object value) {
            this.schema = schema;
            this.value = value;
            this.fieldName = fieldName;
        }

        public SchemaAndValueField assertValueOnlyIf(final Supplier<Boolean> predicate) {
            assertValueOnlyIf = predicate;
            return this;
        }

        public SchemaAndValueField assertWithCondition(final Condition valueCondition) {
            this.valueCondition = valueCondition;
            return this;
        }

        protected void assertFor(Struct content) {
            assertSchema(content);
            assertValue(content);
        }

        private void assertValue(Struct content) {
            if (assertValueOnlyIf != null && !assertValueOnlyIf.get()) {
                return;
            }

            if (value == null) {
                assertNull(fieldName + " is present in the actual content", content.get(fieldName));
                return;
            }
            Object actualValue = content.get(fieldName);

            // assert the value type; for List all implementation types (e.g. immutable ones) are acceptable
            if (actualValue instanceof List) {
                assertTrue("Incorrect value type for " + fieldName, value instanceof List);
                final List<?> actualValueList = (List<?>) actualValue;
                final List<?> valueList = (List<?>) value;
                assertEquals("List size don't match for " + fieldName, valueList.size(), actualValueList.size());
                if (!valueList.isEmpty() && valueList.iterator().next() instanceof Struct) {
                    for (int i = 0; i < valueList.size(); i++) {
                        assertStruct((Struct) valueList.get(i), (Struct) actualValueList.get(i));
                    }
                    return;
                }
            }
            else {
                assertEquals("Incorrect value type for " + fieldName, (value != null) ? value.getClass() : null, (actualValue != null) ? actualValue.getClass() : null);
            }

            if (actualValue instanceof byte[]) {
                assertArrayEquals("Values don't match for " + fieldName, (byte[]) value, (byte[]) actualValue);
            }
            else if (actualValue instanceof Struct) {
                assertStruct((Struct) value, (Struct) actualValue);
            }
            else {
                valueCondition.assertField(fieldName, value, actualValue);
            }
        }

        private void assertStruct(final Struct expectedStruct, final Struct actualStruct) {
            expectedStruct.schema().fields().stream().forEach(field -> {
                final Object expectedValue = expectedStruct.get(field);
                if (expectedValue == null) {
                    assertNull(fieldName + " is present in the actual content", actualStruct.get(field.name()));
                    return;
                }
                final Object actualValue = actualStruct.get(field.name());
                assertNotNull("No value found for " + fieldName, actualValue);
                assertEquals("Incorrect value type for " + fieldName, expectedValue.getClass(), actualValue.getClass());
                if (actualValue instanceof byte[]) {
                    assertArrayEquals("Values don't match for " + fieldName, (byte[]) expectedValue, (byte[]) actualValue);
                }
                else if (actualValue instanceof Struct) {
                    assertStruct((Struct) expectedValue, (Struct) actualValue);
                }
                else {
                    assertEquals("Values don't match for " + fieldName, expectedValue, actualValue);
                }
            });
        }

        private void assertSchema(Struct content) {
            if (schema == null) {
                return;
            }
            Schema schema = content.schema();
            Field field = schema.field(fieldName);
            assertNotNull(fieldName + " not found in schema " + SchemaUtil.asString(schema), field);
            VerifyRecord.assertConnectSchemasAreEqual(field.name(), field.schema(), this.schema);
        }
    }

    protected TestConsumer testConsumer(int expectedRecordsCount, String... topicPrefixes) {
        return new TestConsumer(expectedRecordsCount, topicPrefixes);
    }

    protected class TestConsumer {
        private final ConcurrentLinkedQueue<SourceRecord> records;
        private int expectedRecordsCount;
        private final List<String> topicPrefixes;
        private boolean ignoreExtraRecords = false;

        protected TestConsumer(int expectedRecordsCount, String... topicPrefixes) {
            this.expectedRecordsCount = expectedRecordsCount;
            this.records = new ConcurrentLinkedQueue<>();
            this.topicPrefixes = Arrays.stream(topicPrefixes)
                    .map(p -> TestHelper.TEST_SERVER + "." + p)
                    .collect(Collectors.toList());
        }

        public void setIgnoreExtraRecords(boolean ignoreExtraRecords) {
            this.ignoreExtraRecords = ignoreExtraRecords;
        }

        public void accept(SourceRecord record) {
            if (ignoreTopic(record.topic())) {
                return;
            }

            if (records.size() >= expectedRecordsCount) {
                addRecord(record);
                if (!ignoreExtraRecords) {
                    fail("received more events than expected");
                }
            }
            else {
                addRecord(record);
            }
        }

        private void addRecord(SourceRecord record) {
            records.add(record);
            if (Testing.Debug.isEnabled()) {
                Testing.debug("Consumed record " + records.size() + " / " + expectedRecordsCount + " ("
                        + (expectedRecordsCount - records.size()) + " more)");
                Testing.debug(record);
            }
            else if (Testing.Print.isEnabled()) {
                Testing.print("Consumed record " + records.size() + " / " + expectedRecordsCount + " ("
                        + (expectedRecordsCount - records.size()) + " more)");
                Testing.print(record);
            }
        }

        private boolean ignoreTopic(String topicName) {
            if (topicPrefixes.isEmpty()) {
                return false;
            }

            for (String prefix : topicPrefixes) {
                if (topicName.startsWith(prefix)) {
                    return false;
                }
            }

            return true;
        }

        protected void expects(int expectedRecordsCount) {
            this.expectedRecordsCount = expectedRecordsCount;
        }

        protected SourceRecord remove() {
            return records.remove();
        }

        protected boolean isEmpty() {
            return records.isEmpty();
        }

        protected void process(Consumer<SourceRecord> consumer) {
            records.forEach(consumer);
        }

        protected void clear() {
            records.clear();
        }

        protected void await(long timeout, TimeUnit unit) throws InterruptedException {
            final ElapsedTimeStrategy timer = ElapsedTimeStrategy.constant(Clock.SYSTEM, unit.toMillis(timeout));
            while (!timer.hasElapsed()) {
                final SourceRecord r = consumeRecord();
                if (r != null) {
                    accept(r);
                    if (records.size() == expectedRecordsCount) {
                        break;
                    }
                }
            }
            if (records.size() != expectedRecordsCount) {
                fail("Consumer is still expecting " + (expectedRecordsCount - records.size()) + " records, as it received only " + records.size());
            }
        }
    }

    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("postgres", "test_server");
    }

    protected void waitForSnapshotWithCustomMetricsToBeCompleted(Map<String, String> props) throws InterruptedException {
        waitForSnapshotWithCustomMetricsToBeCompleted("postgres", "test_server", props);
    }

    protected void waitForStreamingToStart() throws InterruptedException {
        waitForStreamingRunning("postgres", "test_server");
    }

    protected void waitForStreamingWithCustomMetricsToStart(Map<String, String> props) throws InterruptedException {
        waitForStreamingWithCustomMetricsToStart("postgres", "test_server", props);
    }

    protected void assertWithTask(Consumer<SourceTask> consumer) {
        engine.runWithTask(consumer);
    }

    protected Map<String, List<SourceRecord>> recordsByTopic(final int expectedRecordsCount, TestConsumer consumer) {
        final Map<String, List<SourceRecord>> recordsByTopic = new HashMap<>();
        for (int i = 0; i < expectedRecordsCount; i++) {
            final SourceRecord record = consumer.remove();
            recordsByTopic.putIfAbsent(record.topic(), new ArrayList<SourceRecord>());
            recordsByTopic.compute(record.topic(), (k, v) -> {
                v.add(record);
                return v;
            });
        }
        return recordsByTopic;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.junit.Assert;

import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.function.BlockingConsumer;
import io.debezium.relational.TableId;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.VariableLatch;

/**
 * Base class for the integration tests for the different {@link RecordsProducer} instances
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public abstract class AbstractRecordsProducerTest {

    protected static final Pattern INSERT_TABLE_MATCHING_PATTERN = Pattern.compile("insert into (.*)\\(.*\\) VALUES .*", Pattern.CASE_INSENSITIVE);

    protected static final String INSERT_CASH_TYPES_STMT = "INSERT INTO cash_table (csh) VALUES ('$1234.11')";
    protected static final String INSERT_DATE_TIME_TYPES_STMT = "INSERT INTO time_table(ts, tz, date, ti, ttz, it) " +
                                                                "VALUES ('2016-11-04T13:51:30'::TIMESTAMP, '2016-11-04T13:51:30+02:00'::TIMESTAMPTZ, " +
                                                                "'2016-11-04'::DATE, '13:51:30'::TIME, '13:51:30+02:00'::TIMETZ, 'P1Y2M3DT4H5M0S'::INTERVAL)";
    protected static final String INSERT_BIN_TYPES_STMT = "INSERT INTO bitbin_table (ba, bol, bs, bv) " +
                                                          "VALUES (E'\\\\001\\\\002\\\\003'::bytea, '0'::bit(1), '11'::bit(2), '00'::bit(2))";
    protected static final String INSERT_GEOM_TYPES_STMT = "INSERT INTO geom_table(p) VALUES ('(1,1)'::point)";
    protected static final String INSERT_TEXT_TYPES_STMT = "INSERT INTO text_table(j, jb, x, u) " +
                                                           "VALUES ('{\"bar\": \"baz\"}'::json, '{\"bar\": \"baz\"}'::jsonb, " +
                                                           "'<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID)";
    protected static final String INSERT_STRING_TYPES_STMT = "INSERT INTO string_table (vc, vcv, ch, c, t) " +
                                                             "VALUES ('aa', 'bb', 'cdef', 'abc', 'some text')";
    protected static final String INSERT_NUMERIC_TYPES_STMT = "INSERT INTO numeric_table (si, i, bi, r, db, ss, bs, b) " +
                                                              "VALUES (1, 123456, 1234567890123, 3.3, 4.44, 1, 123, true)";
    protected static final String INSERT_FP_TYPES_NO_DECIMAL_STMT = "INSERT INTO numeric_table (si, i, bi, r, db, ss, bs, b) " +
                                                                    "VALUES (1, 123456, 1234567890123, 3, 4, 1, 123, true)";
    protected static final String INSERT_NUMERIC_DECIMAL_TYPES_STMT = "INSERT INTO numeric_decimal_table (d, dzs, dvs, n, nzs, nvs) " +
                                                                      "VALUES (1.1, 10.11, 10.1111, 22.22, 22.2, 22.2222)";
    protected static final String INSERT_NUMERIC_DECIMAL_TYPES_NO_DECIMAL_STMT = "INSERT INTO numeric_decimal_table (d, dzs, dvs, n, nzs, nvs) " +
                                                                                 "VALUES (1, 10, 10, 22, 22, 22)";

    protected static final String INSERT_TSTZRANGE_TYPES_STMT = "INSERT INTO tstzrange_table (unbounded_exclusive_range, bounded_inclusive_range) " +
            "VALUES ('[2017-06-05 11:29:12.549426+00,)', '[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]')";


    protected static final String INSERT_ARRAY_TYPES_STMT = "INSERT INTO array_table (int_array, bigint_array, text_array, char_array, varchar_array, date_array, numeric_array, varnumeric_array) " +
                                                             "VALUES ('{1,2,3}', '{1550166368505037572}', '{\"one\",\"two\",\"three\"}', '{\"cone\",\"ctwo\",\"cthree\"}', '{\"vcone\",\"vctwo\",\"vcthree\"}', '{2016-11-04,2016-11-05,2016-11-06}', '{1.2,3.4,5.6}', '{1.1,2.22,3.333}')";

    protected static final String INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT = "INSERT INTO array_table_with_nulls (int_array, bigint_array, text_array, date_array, numeric_array, varnumeric_array) " +
            "VALUES (null, null, null, null, null, null)";

    protected static final String INSERT_POSTGIS_TYPES_STMT = "INSERT INTO public.postgis_table (p, ml) " +
            "VALUES ('SRID=3187;POINT(174.9479 -36.7208)'::postgis.geometry, 'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::postgis.geography)";

    protected static final String INSERT_POSTGIS_ARRAY_TYPES_STMT = "INSERT INTO public.postgis_array_table (ga) " +
            "VALUES (ARRAY['GEOMETRYCOLLECTION EMPTY'::postgis.geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::postgis.geometry])";

    protected static final String INSERT_QUOTED_TYPES_STMT = "INSERT INTO \"Quoted_\"\" . Schema\".\"Quoted_\"\" . Table\" (\"Quoted_\"\" . Text_Column\") " +
                                                             "VALUES ('some text')";

    protected static final String INSERT_CUSTOM_TYPES_STMT = "INSERT INTO custom_table (lt, i, n) " +
            "VALUES ('Top.Collections.Pictures.Astronomy.Galaxies', '978-0-393-04002-9', NULL)";

    protected static final Set<String> ALL_STMTS = new HashSet<>(Arrays.asList(INSERT_NUMERIC_TYPES_STMT, INSERT_NUMERIC_DECIMAL_TYPES_STMT,
                                                                 INSERT_DATE_TIME_TYPES_STMT,
                                                                 INSERT_BIN_TYPES_STMT, INSERT_GEOM_TYPES_STMT, INSERT_TEXT_TYPES_STMT,
                                                                 INSERT_CASH_TYPES_STMT, INSERT_STRING_TYPES_STMT, INSERT_ARRAY_TYPES_STMT,
                                                                 INSERT_ARRAY_TYPES_WITH_NULL_VALUES_STMT, INSERT_QUOTED_TYPES_STMT,
                                                                 INSERT_POSTGIS_TYPES_STMT, INSERT_POSTGIS_ARRAY_TYPES_STMT));

    protected List<SchemaAndValueField> schemasAndValuesForNumericType() {
        return Arrays.asList(new SchemaAndValueField("si", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("i", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123456),
                             new SchemaAndValueField("bi", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 1234567890123L),
                             new SchemaAndValueField("r", Schema.OPTIONAL_FLOAT32_SCHEMA, 3.3f),
                             new SchemaAndValueField("db", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.44d),
                             new SchemaAndValueField("ss", Schema.INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("bs", Schema.INT64_SCHEMA, 123L),
                             new SchemaAndValueField("b", Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.TRUE));
    }

    protected List<SchemaAndValueField> schemasAndValuesForFpTypeWithoutDecimals() {
        return Arrays.asList(new SchemaAndValueField("si", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("i", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123456),
                             new SchemaAndValueField("bi", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 1234567890123L),
                             new SchemaAndValueField("r", Schema.OPTIONAL_FLOAT32_SCHEMA, 3.0f),
                             new SchemaAndValueField("db", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.0d),
                             new SchemaAndValueField("ss", Schema.INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("bs", Schema.INT64_SCHEMA, 123L),
                             new SchemaAndValueField("b", Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.TRUE));
    }

    protected List<SchemaAndValueField> schemasAndValuesForNumericDecimalType() {
        final Struct dvs = new Struct(VariableScaleDecimal.schema());
        dvs.put("scale", 4).put("value", new BigDecimal("10.1111").unscaledValue().toByteArray());
        final Struct nvs = new Struct(VariableScaleDecimal.schema());
        nvs.put("scale", 4).put("value", new BigDecimal("22.2222").unscaledValue().toByteArray());
        return Arrays.asList(
                new SchemaAndValueField("d", Decimal.builder(2).optional().build(), new BigDecimal("1.10")),
     // DBZ-351 new SchemaAndValueField("dzs", Decimal.builder(0).optional().build(), new BigDecimal("10")),
                new SchemaAndValueField("dvs", VariableScaleDecimal.builder().optional().build(), dvs),
                new SchemaAndValueField("n", Decimal.builder(4).optional().build(), new BigDecimal("22.2200")),
     // DBZ-351 new SchemaAndValueField("nzs", Decimal.builder(0).optional().build(), new BigDecimal("22")),
                new SchemaAndValueField("nvs", VariableScaleDecimal.builder().optional().build(), nvs)
        );
    }

    protected List<SchemaAndValueField> schemasAndValuesForNumericDecimalTypeWithoutDecimals() {
        final Struct dvs = new Struct(VariableScaleDecimal.schema());
        dvs.put("scale", 1).put("value", new BigDecimal("10.0").unscaledValue().toByteArray());
        final Struct nvs = new Struct(VariableScaleDecimal.schema());
        nvs.put("scale", 1).put("value", new BigDecimal("22.0").unscaledValue().toByteArray());
        return Arrays.asList(
                new SchemaAndValueField("d", Decimal.builder(2).optional().build(), new BigDecimal("1.00")),
     // DBZ-351 new SchemaAndValueField("dzs", Decimal.builder(0).optional().build(), new BigDecimal("10")),
                new SchemaAndValueField("dvs", VariableScaleDecimal.builder().optional().build(), dvs),
                new SchemaAndValueField("n", Decimal.builder(4).optional().build(), new BigDecimal("22.0000")),
     // DBZ-351 new SchemaAndValueField("nzs", Decimal.builder(0).optional().build(), new BigDecimal("22")),
                new SchemaAndValueField("nvs", VariableScaleDecimal.builder().optional().build(), nvs)
        );
    }

    protected List<SchemaAndValueField> schemasAndValuesForImpreciseNumericDecimalType() {
        return Arrays.asList(
                new SchemaAndValueField("d", Schema.OPTIONAL_FLOAT64_SCHEMA, 1.1d),
                new SchemaAndValueField("dzs", Schema.OPTIONAL_FLOAT64_SCHEMA, 10d),
                new SchemaAndValueField("dvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 10.1111d),
                new SchemaAndValueField("n", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.22d),
                new SchemaAndValueField("nzs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22d),
                new SchemaAndValueField("nvs", Schema.OPTIONAL_FLOAT64_SCHEMA, 22.2222d)
        );
    }

    protected List<SchemaAndValueField> schemasAndValuesForStringTypes() {
       return Arrays.asList(new SchemaAndValueField("vc", Schema.OPTIONAL_STRING_SCHEMA, "aa"),
                            new SchemaAndValueField("vcv", Schema.OPTIONAL_STRING_SCHEMA, "bb"),
                            new SchemaAndValueField("ch", Schema.OPTIONAL_STRING_SCHEMA, "cdef"),
                            new SchemaAndValueField("c", Schema.OPTIONAL_STRING_SCHEMA, "abc"),
                            new SchemaAndValueField("t", Schema.OPTIONAL_STRING_SCHEMA, "some text"));
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

    protected List<SchemaAndValueField> schemaAndValuesForTstzRangeTypes() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSx");
        Instant begin = dateTimeFormatter.parse("2017-06-05 11:29:12.549426+00", Instant::from);
        Instant end = dateTimeFormatter.parse("2017-06-05 12:34:56.789012+00", Instant::from);

        // Acknowledge timezone expectation of the system running the test
        String beginSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(begin);
        String endSystemTime = dateTimeFormatter.withZone(ZoneId.systemDefault()).format(end);

        String expectedField1 = String.format("[\"%s\",)", beginSystemTime);
        String expectedField2 = String.format("[\"%s\",\"%s\"]", beginSystemTime, endSystemTime);

        return Arrays.asList(
                new SchemaAndValueField("unbounded_exclusive_range", Schema.OPTIONAL_STRING_SCHEMA, expectedField1),
                new SchemaAndValueField("bounded_inclusive_range", Schema.OPTIONAL_STRING_SCHEMA, expectedField2)
        );
    }

    protected List<SchemaAndValueField> schemaAndValuesForBinTypes() {
       return Arrays.asList(new SchemaAndValueField("ba", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 1, 2, 3})),
                            new SchemaAndValueField("bol", Schema.OPTIONAL_BOOLEAN_SCHEMA, false),
                            new SchemaAndValueField("bs", Bits.builder(2).optional().build(), new byte[] { 3, 0 }),  // bitsets get converted from two's complement
                            new SchemaAndValueField("bv", Bits.builder(2).optional().build(), new byte[] { 0, 0 }));
    }

    protected List<SchemaAndValueField> schemaAndValuesForDateTimeTypes() {
        long expectedTs = NanoTimestamp.toEpochNanos(LocalDateTime.parse("2016-11-04T13:51:30"), null);
        String expectedTz = "2016-11-04T11:51:30Z"; //timestamp is stored with TZ, should be read back with UTC
        int expectedDate = Date.toEpochDay(LocalDate.parse("2016-11-04"), null);
        long expectedTi = LocalTime.parse("13:51:30").toNanoOfDay();
        String expectedTtz = "11:51:30Z";  //time is stored with TZ, should be read back at GMT
        double interval = MicroDuration.durationMicros(1, 2, 3, 4, 5, 0, PostgresValueConverter.DAYS_PER_MONTH_AVG);

        return Arrays.asList(new SchemaAndValueField("ts", NanoTimestamp.builder().optional().build(), expectedTs),
                             new SchemaAndValueField("tz", ZonedTimestamp.builder().optional().build(), expectedTz),
                             new SchemaAndValueField("date", Date.builder().optional().build(), expectedDate),
                             new SchemaAndValueField("ti", NanoTime.builder().optional().build(), expectedTi),
                             new SchemaAndValueField("ttz", ZonedTime.builder().optional().build(), expectedTtz),
                             new SchemaAndValueField("it", MicroDuration.builder().optional().build(), interval));
    }

    protected List<SchemaAndValueField> schemaAndValuesForDateTimeTypesAdaptiveTimeMicroseconds() {
        long expectedTs = NanoTimestamp.toEpochNanos(LocalDateTime.parse("2016-11-04T13:51:30"), null);
        String expectedTz = "2016-11-04T11:51:30Z"; //timestamp is stored with TZ, should be read back with UTC
        int expectedDate = Date.toEpochDay(LocalDate.parse("2016-11-04"), null);
        long expectedTi = LocalTime.parse("13:51:30").toNanoOfDay() / 1_000;
        String expectedTtz = "11:51:30Z";  //time is stored with TZ, should be read back at GMT
        double interval = MicroDuration.durationMicros(1, 2, 3, 4, 5, 0, PostgresValueConverter.DAYS_PER_MONTH_AVG);

        return Arrays.asList(new SchemaAndValueField("ts", NanoTimestamp.builder().optional().build(), expectedTs),
                new SchemaAndValueField("tz", ZonedTimestamp.builder().optional().build(), expectedTz),
                new SchemaAndValueField("date", Date.builder().optional().build(), expectedDate),
                new SchemaAndValueField("ti", MicroTime.builder().optional().build(), expectedTi),
                new SchemaAndValueField("ttz", ZonedTime.builder().optional().build(), expectedTtz),
                new SchemaAndValueField("it", MicroDuration.builder().optional().build(), interval));
    }

    protected List<SchemaAndValueField> schemaAndValuesForMoneyTypes() {
        return Collections.singletonList(new SchemaAndValueField("csh", Decimal.builder(0).optional().build(),
                                                                 BigDecimal.valueOf(1234.11d)));
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
                                        (int)LocalDate.of(2016, Month.NOVEMBER, 4).toEpochDay(),
                                        (int)LocalDate.of(2016, Month.NOVEMBER, 5).toEpochDay(),
                                        (int)LocalDate.of(2016, Month.NOVEMBER, 6).toEpochDay()
                                )),
                            new SchemaAndValueField("numeric_array", SchemaBuilder.array(Decimal.builder(2).optional().build()).optional().build(),
                                    Arrays.asList(
                                            new BigDecimal("1.20"),
                                            new BigDecimal("3.40"),
                                            new BigDecimal("5.60")
                                    )),
                            new SchemaAndValueField("varnumeric_array", SchemaBuilder.array(VariableScaleDecimal.builder().optional().build()).optional().build(),
                                    varnumArray)
                            );
    }

    protected List<SchemaAndValueField> schemasAndValuesForArrayTypesWithNullValues() {
        return Arrays.asList(
                new SchemaAndValueField("int_array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), null),
                new SchemaAndValueField("bigint_array", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(), null),
                new SchemaAndValueField("text_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("char_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("varchar_array", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(), null),
                new SchemaAndValueField("date_array", SchemaBuilder.array(Date.builder().optional().schema()).optional().build(), null),
                new SchemaAndValueField("numeric_array", SchemaBuilder.array(Decimal.builder(2).optional().build()).optional().build(), null)
        );
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
                        Geography.createValue(geogSchema, DatatypeConverter.parseHexBinary("0105000020E610000001000000010200000002000000A779C7293A2465400B462575025A46C0C66D3480B7FC6440C3D32B65195246C0"), 4326))
        );
    }

    protected List<SchemaAndValueField> schemaAndValuesForPostgisArrayTypes() {
        Schema geomSchema = Geometry.builder().build();
        return Arrays.asList(
                // geometries are encoded here as HexEWKB
                new SchemaAndValueField("ga", SchemaBuilder.array(geomSchema).optional().build(),
                        Arrays.asList(
                                // 'GEOMETRYCOLLECTION EMPTY'::postgis.geometry
                                Geometry.createValue(geomSchema, DatatypeConverter.parseHexBinary("010700000000000000"), null),
                                // 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::postgis.geometry
                                Geometry.createValue(geomSchema, DatatypeConverter.parseHexBinary("01030000000100000005000000B81E85EB51D0644052B81E85EB5147C0713D0AD7A350664052B81E85EB5147C0713D0AD7A35066409A999999993941C0B81E85EB51D064409A999999993941C0B81E85EB51D0644052B81E85EB5147C0"), null)
                        )
                )
        );
    }

    protected List<SchemaAndValueField> schemasAndValuesForQuotedTypes() {
       return Arrays.asList(new SchemaAndValueField("Quoted_\" . Text_Column", Schema.OPTIONAL_STRING_SCHEMA, "some text"));
    }

    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTableName() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::tableNameFromInsertStmt,
                                                           this::schemasAndValuesForTable));
    }

    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTableNameAdaptiveTimeMicroseconds() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::tableNameFromInsertStmt,
                this::schemasAndValuesForTableAdaptiveTimeMicroseconds));
    }

    protected List<SchemaAndValueField> schemasAndValuesForTableAdaptiveTimeMicroseconds(String insertTableStatement) {
        if (insertTableStatement.equals(INSERT_DATE_TIME_TYPES_STMT)) {
            return schemaAndValuesForDateTimeTypesAdaptiveTimeMicroseconds();
        }
        return schemasAndValuesForTable(insertTableStatement);
    }

    protected List<SchemaAndValueField> schemasAndValuesForCustomTypes() {
        return Arrays.asList(new SchemaAndValueField("lt", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("Top.Collections.Pictures.Astronomy.Galaxies".getBytes())),
                             new SchemaAndValueField("i", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap("0-393-04002-X".getBytes())),
                             new SchemaAndValueField("n", Schema.OPTIONAL_STRING_SCHEMA, null));

    }

    protected List<SchemaAndValueField> schemasAndValuesForTable(String insertTableStatement) {
        switch (insertTableStatement) {
            case INSERT_NUMERIC_TYPES_STMT:
                return schemasAndValuesForNumericType();
            case INSERT_NUMERIC_DECIMAL_TYPES_STMT:
                return schemasAndValuesForNumericDecimalType();
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
        assertNotNull("expected there to be content in Envelope under " + envelopeFieldName, content);
        expectedSchemaAndValuesByColumn.forEach(schemaAndValueField -> schemaAndValueField.assertFor(content));
    }

    protected void assertRecordOffset(SourceRecord record, boolean expectSnapshot, boolean expectedLastSnapshotRecord) {
        Map<String, ?> offset = record.sourceOffset();
        assertNotNull(offset.get(SourceInfo.TXID_KEY));
        assertNotNull(offset.get(SourceInfo.TIMESTAMP_KEY));
        assertNotNull(offset.get(SourceInfo.LSN_KEY));
        Object snapshot = offset.get(SourceInfo.SNAPSHOT_KEY);
        Object lastSnapshotRecord = offset.get(SourceInfo.LAST_SNAPSHOT_RECORD_KEY);
        if (expectSnapshot) {
            Assert.assertTrue("Snapshot marker expected but not found", (Boolean) snapshot);
            assertEquals("Last snapshot record marker mismatch", expectedLastSnapshotRecord, lastSnapshotRecord);
        } else {
            assertNull("Snapshot marker not expected, but found", snapshot);
            assertNull("Last snapshot marker not expected, but found", lastSnapshotRecord);
        }
    }

    protected static String tableNameFromInsertStmt(String statement) {
        return tableIdFromInsertStmt(statement).toString();
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

    protected static class SchemaAndValueField {
        private final Object schema;
        private final Object value;
        private final String fieldName;
        private Supplier<Boolean> assertValueOnlyIf = null;

        public SchemaAndValueField(String fieldName, Object schema, Object value) {
            this.schema = schema;
            this.value = value;
            this.fieldName = fieldName;
        }

        public SchemaAndValueField assertValueOnlyIf(final Supplier<Boolean> predicate) {
            assertValueOnlyIf = predicate;
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
            if(actualValue instanceof List) {
                assertTrue("Incorrect value type for " + fieldName, value instanceof List);
                final List<?> actualValueList = (List<?>)actualValue;
                final List<?> valueList = (List<?>)value;
                assertEquals("List size don't match for " + fieldName, valueList.size(), actualValueList.size());
                if (!valueList.isEmpty() && valueList.iterator().next() instanceof Struct) {
                    for (int i = 0; i < valueList.size(); i++) {
                        assertStruct((Struct)valueList.get(i), (Struct)actualValueList.get(i));
                    }
                    return;
                }
            }
            else {
                assertEquals("Incorrect value type for " + fieldName, value.getClass(), actualValue.getClass());
            }

            if (actualValue instanceof byte[]) {
                assertArrayEquals("Values don't match for " + fieldName, (byte[]) value, (byte[]) actualValue);
            } else if (actualValue instanceof Struct) {
                assertStruct((Struct)value, (Struct)actualValue);
            } else {
                assertEquals("Values don't match for " + fieldName, value, actualValue);
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
                } else if (actualValue instanceof Struct) {
                    assertStruct((Struct)expectedValue, (Struct)actualValue);
                } else {
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
            assertNotNull(fieldName + " not found in schema " + schema, field);
            assertEquals("Schema for " + field.name() + " does not match the actual value", this.schema, field.schema());
        }
    }

    protected TestConsumer testConsumer(int expectedRecordsCount, String... topicPrefixes) {
         return new TestConsumer(expectedRecordsCount, topicPrefixes);
    }

    protected static class TestConsumer implements BlockingConsumer<ChangeEvent> {
        private final ConcurrentLinkedQueue<SourceRecord> records;
        private final VariableLatch latch;
        private final List<String> topicPrefixes;
        private boolean ignoreExtraRecords = false;

        protected TestConsumer(int expectedRecordsCount, String... topicPrefixes) {
            this.latch = new VariableLatch(expectedRecordsCount);
            this.records = new ConcurrentLinkedQueue<>();
            this.topicPrefixes = Arrays.stream(topicPrefixes)
                    .map(p -> TestHelper.TEST_SERVER + "." + p)
                    .collect(Collectors.toList());
        }

        public void setIgnoreExtraRecords(boolean ignoreExtraRecords) {
            this.ignoreExtraRecords = ignoreExtraRecords;
        }

        @Override
        public void accept(ChangeEvent event) {
            final SourceRecord record = event.getRecord();
            if ( ignoreTopic(record.topic()) ) {
                return;
            }

            if (latch.getCount() == 0) {
                if (ignoreExtraRecords) {
                    records.add(record);
                } else {
                    fail("received more events than expected");
                }
            } else {
                records.add(record);
                latch.countDown();
            }
        }

        private boolean ignoreTopic(String topicName) {
            if (topicPrefixes.isEmpty()) {
                return false;
            }

            for (String prefix : topicPrefixes) {
                if ( topicName.startsWith(prefix)) {
                    return false;
                }
            }

            return true;
        }

        protected void expects(int expectedRecordsCount) {
            assert latch.getCount() == 0;
            this.latch.countUp(expectedRecordsCount);
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
            if (!latch.await(timeout, unit)) {
                fail("Consumer expected " + latch.getCount() + " records, but received " + records.size());
            }
        }
    }
}

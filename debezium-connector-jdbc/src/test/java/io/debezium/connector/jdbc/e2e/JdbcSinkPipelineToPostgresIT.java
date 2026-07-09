/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PGobject;

import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.WithPostgresExtension;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.WithTemporalPrecisionMode;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.spatial.GeometryBytes;
import io.debezium.util.HexConverter;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to PostgreSQL.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-postgresql")
@ExtendWith(PostgresSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToPostgresIT extends AbstractJdbcSinkPipelineIT {

    @Override
    protected boolean isBitCoercedToBoolean() {
        return true;
    }

    @Override
    protected String getBooleanType() {
        return "BOOL";
    }

    @Override
    protected String getBitsDataType() {
        return "BIT";
    }

    @Override
    protected String getInt8Type() {
        return "INT2";
    }

    @Override
    protected String getInt16Type() {
        return "INT2";
    }

    @Override
    protected String getInt32Type() {
        return "INT4";
    }

    @Override
    protected String getInt64Type() {
        return "INT8";
    }

    @Override
    protected String getVariableScaleDecimalType() {
        return "FLOAT8";
    }

    @Override
    protected String getDecimalType() {
        return "NUMERIC";
    }

    @Override
    protected String getFloat32Type() {
        return "FLOAT4";
    }

    @Override
    protected String getFloat64Type() {
        return "FLOAT8";
    }

    @Override
    protected String getCharType(Source source, boolean key, boolean nationalized) {
        if (source.getOptions().isColumnTypePropagated() && !key) {
            // Debezium does not propagate column type details for keys.
            // PostgreSQL driver returns BPCHAR standing for Blank Padded Character type.
            return "BPCHAR";
        }
        return getTextType();
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (maxLength) {
            return getTextType(nationalized);
        }
        if (!source.getOptions().isColumnTypePropagated() || key) {
            // Debezium does not propagate column type details for keys.
            return getTextType();
        }
        return "VARCHAR";
    }

    @Override
    protected String getTextType(boolean nationalized) {
        return "TEXT";
    }

    @Override
    protected String getBinaryType(Source source, String sourceDataType) {
        return "BYTEA";
    }

    @Override
    protected String getJsonType(Source source) {
        return "JSON";
    }

    @Override
    protected String getJsonbType(Source source) {
        if (source.getOptions().isColumnTypePropagated()) {
            return "JSONB";
        }
        return getJsonType(source);
    }

    @Override
    protected String getXmlType(Source source) {
        if (source.getType() == SourceType.POSTGRES || source.getOptions().isColumnTypePropagated()) {
            // XML is always emitted as io.debezium.data.Xml from PostgreSQL sources
            return "XML";
        }
        return "TEXT";
    }

    @Override
    protected String getUuidType(Source source, boolean key) {
        return "UUID";
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        // The io.debezium.data.Enum implementation does not pass data type names, so we cannot replicate
        // the enum in the destination system, so we have to assume that STRING fallback is the all that
        // can be resolved.
        return getTextType();
    }

    @Override
    protected String getSetType(Source source, boolean key) {
        return getTextType();
    }

    @Override
    protected String getYearType() {
        return getInt32Type();
    }

    @Override
    protected String getDateType() {
        return "DATE";
    }

    @Override
    protected String getTimeType(Source source, boolean key, int precision) {
        return "TIME";
    }

    @Override
    protected String getTimeWithTimezoneType(Source source, boolean key, int precision) {
        return "TIMETZ";
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        return "TIMESTAMP";
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        return "TIMESTAMPTZ";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return "INTERVAL";
    }

    @Override
    protected String getGeographyType() {
        return "\"postgis\".\"geography\"";
    }

    @Override
    protected String getGeometryType() {
        return "\"postgis\".\"geometry\"";
    }

    @Override
    protected GeometryBytes getGeometryValues(ResultSet resultSet, int index) throws SQLException {
        final PGobject object = (PGobject) resultSet.getObject(index);
        if (object == null || object.getValue() == null) {
            return null;
        }
        // Tests expect WKB so convert it from EWKB that Postgres provides
        return new GeometryBytes(HexConverter.convertFromHex(object.getValue())).asWkb();
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "PostgreSQL source emits multi-byte BIT values as Debezium Bits")
    public void testBitDataTypeWithMultiByteValues(Source source, Sink sink) throws Exception {
        assertDataTypesNonKeyOnly(source,
                sink,
                List.of("bit(8)", "bit(16)", "bit(24)"),
                List.of("B'11111111'", "B'0000000100000010'", "B'000000110000001000000001'"),
                List.of("11111111", "0000000100000010", "000000110000001000000001"),
                (record) -> {
                    assertColumn(sink, record, "data0", getBitsDataType(), 8);
                    assertColumn(sink, record, "data1", getBitsDataType(), 16);
                    assertColumn(sink, record, "data2", getBitsDataType(), 24);
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "PostgreSQL source emits multi-byte VARBIT values as Debezium Bits")
    public void testBitVaryingDataTypeWithMultiByteValues(Source source, Sink sink) throws Exception {
        assertDataTypesNonKeyOnly(source,
                sink,
                List.of("bit varying(8)", "bit varying(16)", "bit varying(24)"),
                List.of("B'11111111'", "B'0000000100000010'", "B'000000110000001000000001'"),
                List.of("11111111", "0000000100000010", "000000110000001000000001"),
                (record) -> {
                    final String dataType = source.getOptions().isColumnTypePropagated() ? "VARBIT" : getBitsDataType();
                    assertColumn(sink, record, "data0", dataType, 8);
                    assertColumn(sink, record, "data1", dataType, 16);
                    assertColumn(sink, record, "data2", dataType, 24);
                },
                ResultSet::getString);
    }

    @TestTemplate
    @FixFor("debezium/dbz#2100")
    @ForSource(value = SourceType.POSTGRES, reason = "PostgreSQL TIME allows 24:00:00 as a boundary value")
    @WithTemporalPrecisionMode(include = {
            TemporalPrecisionMode.ADAPTIVE,
            TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
            TemporalPrecisionMode.MICROSECONDS,
            TemporalPrecisionMode.NANOSECONDS
    })
    public void testTimeDataTypeWithBoundaryValue(Source source, Sink sink) throws Exception {
        final List<String> typeNames = List.of("time(0)", "time(1)", "time(2)", "time(3)", "time(4)", "time(5)", "time(6)");
        final List<String> values = List.of(
                "'23:59:59.999999'",
                "'23:59:59.999999'",
                "'23:59:59.999999'",
                "'23:59:59.999999'",
                "'23:59:59.999999'",
                "'23:59:59.999999'",
                "'24:00:00'");

        assertDataTypes2(source,
                sink,
                typeNames,
                values,
                Collections.nCopies(14, "24:00:00"),
                (record) -> {
                    for (int i = 0; i < typeNames.size(); ++i) {
                        assertColumn(sink, record, "id" + i, getTimeType(source, true, i));
                        assertColumn(sink, record, "data" + i, getTimeType(source, false, i));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The infinity value is valid only for PostgreSQL")
    @WithTemporalPrecisionMode
    @Override
    public void testTimestampWithTimeZoneDataTypeWithInfinityValue(Source source, Sink sink) throws Exception {

        final List<String> values = List.of("'-infinity'", "'infinity'");

        List<String> expectedValues = values.stream()
                .map(s -> s.replace("'", ""))
                .collect(Collectors.toList());

        assertDataTypesNonKeyOnly(source,
                sink,
                List.of("timestamptz", "timestamptz"),
                values,
                expectedValues,
                (record) -> {
                    assertColumn(sink, record, "data0", getTimestampWithTimezoneType(source, false, 6));
                    assertColumn(sink, record, "data1", getTimestampWithTimezoneType(source, false, 6));
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "The SPARSEVEC data type only applies to PostgreSQL")
    @WithPostgresExtension("vector")
    public void testSparseVectorDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "sparsevec(25)",
                List.of("'{1:0.1,3:0.2,5:0.3}/25'"),
                List.of("{1:0.1,3:0.2,5:0.3}/25"),
                (record) -> assertColumn(sink, record, "data", "SPARSEVEC"),
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "The HALFVEC data type only applies to PostgreSQL")
    @WithPostgresExtension("vector")
    public void testHalfVectorDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "halfvec(3)",
                List.of("'[101,102,103]'"),
                List.of("[101,102,103]"),
                (record) -> assertColumn(sink, record, "data", "HALFVEC"),
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "The tsvector data type only applies to PostgreSQL")
    public void testTsvectorDataTypeWithStaticValue(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "tsvector",
                List.of("'full:3 postgre:1 search:5 support:2 text:4'"),
                List.of("'full':3 'postgre':1 'search':5 'support':2 'text':4"),
                (record) -> assertColumn(sink, record, "data", "tsvector"),
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "The tsvector data type only applies to PostgreSQL")
    public void testTsvectorDataTypeWithDirectFunctionInsert(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "tsvector",
                List.of("to_tsvector('english', 'This is a test for direct tsvector insert')"),
                List.of("'direct':6 'insert':8 'test':4 'tsvector':7"),
                (record) -> assertColumn(sink, record, "data", "tsvector"),
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = SourceType.POSTGRES, reason = "STRUCTURED-mode TIMETZ raw fidelity (offset + 24:00 boundary) is PostgreSQL specific")
    @WithTemporalPrecisionMode(include = TemporalPrecisionMode.STRUCTURED)
    @Disabled("Requires a source connector runtime with STRUCTURED temporal support. The e2e source pipeline runs on "
            + "the Debezium nightly Connect image, whose PostgreSQL connector does not yet accept "
            + "time.precision.mode=structured; enable once structured temporal support ships in the nightly image. "
            + "Source-side fidelity is covered meanwhile by PostgresTemporalPrecisionHandlingIT and the sink literal by "
            + "StructuredTemporalTypeTest#shouldBindStructuredZonedTimeBoundaryHour24.")
    public void testTimeWithTimeZonePreservesOffsetAndBoundaryInStructuredMode(Source source, Sink sink) throws Exception {
        // PostgreSQL TIMETZ keeps the offset as stored (no session-TZ adjustment) and allows the end-of-day
        // boundary 24:00:00. In STRUCTURED mode both must round-trip to the sink unchanged, which the UTC-normalizing
        // OffsetTime path cannot do (OffsetTime/LocalTime cannot even represent hour 24). Read the sink column as
        // text since hour 24 is not representable by java.time types.
        assertDataTypesNonKeyOnly(source,
                sink,
                List.of("time(6) with time zone", "time(6) with time zone"),
                List.of("'24:00:00+05:30'", "'13:51:30.123789-04:30'"),
                List.of("24:00:00+05:30", "13:51:30.123789-04:30"),
                (record) -> {
                    assertColumn(sink, record, "data0", getTimeWithTimezoneType(source, false, 6));
                    assertColumn(sink, record, "data1", getTimeWithTimezoneType(source, false, 6));
                },
                ResultSet::getString);
    }

}

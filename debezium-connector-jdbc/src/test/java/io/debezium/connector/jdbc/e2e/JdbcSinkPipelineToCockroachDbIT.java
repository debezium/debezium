/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PGobject;

import io.debezium.connector.jdbc.junit.jupiter.CockroachDbSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.WithTemporalPrecisionMode;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.spatial.GeometryBytes;
import io.debezium.util.HexConverter;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to CockroachDB.
 *
 * <p>CockroachDB is wire-compatible with PostgreSQL, so the expectations largely mirror the
 * PostgreSQL pipeline. Differences stem from CockroachDB's type aliases: JSON is an alias of
 * JSONB, character types surface through the pg_catalog as text or varchar, and the spatial
 * types are built in rather than provided by the PostGIS extension.</p>
 *
 * @author Virag Tripathi
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-cockroachdb")
@ExtendWith(CockroachDbSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToCockroachDbIT extends AbstractJdbcSinkPipelineIT {

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
        if (key) {
            // Key strings are created as sized varchar columns on CockroachDB.
            return "VARCHAR";
        }
        if (source.getOptions().isColumnTypePropagated()) {
            // CockroachDB surfaces fixed-length character columns as BPCHAR through pg_catalog.
            return "BPCHAR";
        }
        return getTextType();
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (key) {
            // Key strings are created as sized varchar columns on CockroachDB.
            return "VARCHAR";
        }
        if (maxLength) {
            return getTextType(nationalized);
        }
        if (!source.getOptions().isColumnTypePropagated()) {
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
        // JSON is an alias of JSONB in CockroachDB and is reported as JSONB by the driver.
        return "JSONB";
    }

    @Override
    protected String getJsonbType(Source source) {
        return "JSONB";
    }

    @Override
    protected String getXmlType(Source source) {
        return "TEXT";
    }

    @Override
    protected String getUuidType(Source source, boolean key) {
        return "UUID";
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        if (key) {
            // Key strings are created as sized varchar columns on CockroachDB.
            return "VARCHAR";
        }
        return getTextType();
    }

    @Override
    protected String getSetType(Source source, boolean key) {
        if (key) {
            // Key strings are created as sized varchar columns on CockroachDB.
            return "VARCHAR";
        }
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
        // Spatial types are built into CockroachDB; there is no PostGIS extension schema.
        return "GEOGRAPHY";
    }

    @Override
    protected String getGeometryType() {
        return "GEOMETRY";
    }

    @Override
    protected GeometryBytes getGeometryValues(ResultSet resultSet, int index) throws SQLException {
        final PGobject object = (PGobject) resultSet.getObject(index);
        if (object == null || object.getValue() == null) {
            return null;
        }
        // Tests expect WKB so convert it from EWKB that CockroachDB provides
        return new GeometryBytes(HexConverter.convertFromHex(object.getValue())).asWkb();
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The infinity value is valid only for PostgreSQL")
    @WithTemporalPrecisionMode
    @Override
    public void testTimestampWithTimeZoneDataTypeWithInfinityValue(Source source, Sink sink) throws Exception {

        final List<String> values = List.of("'-infinity'", "'infinity'");

        // CockroachDB round-trips the PostgreSQL infinity literals.
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

}

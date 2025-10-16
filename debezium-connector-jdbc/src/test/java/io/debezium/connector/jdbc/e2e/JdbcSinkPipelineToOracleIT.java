/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.spatial.GeometryBytes;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to Oracle.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-oracle")
@ExtendWith(OracleSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToOracleIT extends AbstractJdbcSinkPipelineIT {

    @Override
    protected String getBooleanType() {
        return "NUMBER";
    }

    @Override
    protected String getBitsDataType() {
        return "BLOB";
    }

    @Override
    protected String getInt8Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt16Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt32Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt64Type() {
        return "NUMBER";
    }

    @Override
    protected String getVariableScaleDecimalType() {
        return "FLOAT";
    }

    @Override
    protected String getDecimalType() {
        return "NUMBER";
    }

    @Override
    protected String getFloat32Type() {
        return "FLOAT";
    }

    @Override
    protected String getFloat64Type() {
        return "FLOAT";
    }

    @Override
    protected String getCharType(Source source, boolean key, boolean nationalized) {
        if (source.getType().is(SourceType.MYSQL)) {
            if (source.getOptions().isColumnTypePropagated() && !key) {
                return nationalized ? "NCHAR" : "CHAR";
            }
            else if (!key) {
                return nationalized ? "NCLOB" : "CLOB";
            }
            return nationalized ? "NVARCHAR2" : "VARCHAR2";
        }
        else {
            // Debezium does not propagate column type details for keys.
            if (source.getOptions().isColumnTypePropagated()) {
                if (!key) {
                    return nationalized ? "NCHAR" : "CHAR";
                }
            }
            else if (!key) {
                // todo: nationalize this?
                return "CLOB";
            }
            // todo: nationalize this?
            return "VARCHAR2";
        }
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (maxLength) {
            return getTextType(nationalized);
        }
        if (source.getType().is(SourceType.MYSQL)) {
            if (source.getOptions().isColumnTypePropagated()) {
                return nationalized ? "NVARCHAR2" : "VARCHAR2";
            }
            else if (key) {
                return nationalized ? "NVARCHAR2" : "VARCHAR2";
            }
            return nationalized ? "NCLOB" : "CLOB";
        }
        else if (!source.getOptions().isColumnTypePropagated()) {
            return !key ? getTextType(false) : "VARCHAR2";
        }
        else if (key) {
            return "VARCHAR2";
        }
        else {
            return nationalized ? "NVARCHAR2" : "VARCHAR2";
        }
    }

    @Override
    protected String getTextType(boolean nationalized) {
        return !nationalized ? "CLOB" : "NCLOB";
    }

    @Override
    protected String getBinaryType(Source source, String sourceDataType) {
        return "BLOB";
    }

    @Override
    protected String getJsonType(Source source) {
        return "CLOB";
    }

    @Override
    protected String getXmlType(Source source) {
        return "CLOB";
    }

    @Override
    protected String getUuidType(Source source, boolean key) {
        return key ? getStringType(source, key, false) : getTextType(false);
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        return key ? getStringType(source, true, false) : getTextType();
    }

    @Override
    protected String getSetType(Source source, boolean key) {
        return key ? getStringType(source, true, false) : getTextType();
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
    protected int getDefaultSinkTimePrecision() {
        // HHH-18035 - Hibernate changed default precision from 6 to 9 in Hibernate 7.0
        return 9;
    }

    @Override
    protected int getMaxTimestampPrecision() {
        // HHH-18035 - Hibernate changed default precision from 6 to 9 in Hibernate 7.0
        return 9;
    }

    @Override
    protected String getTimeType(Source source, boolean key, int precision) {
        if (!source.getOptions().isColumnTypePropagated() || key) {
            precision = getDefaultSinkTimePrecision();
        }

        return String.format("TIMESTAMP(%d)", Math.min(getDefaultSinkTimePrecision(), precision));
    }

    @Override
    protected String getTimeWithTimezoneType(Source source, boolean key, int precision) {
        if (!(source.getOptions().isColumnTypePropagated() && !key)) {
            precision = getMaxTimestampPrecision();
        }
        return String.format("TIMESTAMP(%d) WITH TIME ZONE", precision);
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        if (source.getOptions().isColumnTypePropagated() && !key) {
            return String.format("TIMESTAMP(%d)", precision);
        }
        return "TIMESTAMP(" + getMaxTimestampPrecision() + ")"; // DATE
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        if (source.getOptions().isColumnTypePropagated() && !key) {
            return String.format("TIMESTAMP(%d) WITH TIME ZONE", precision);
        }
        return "TIMESTAMP(" + getMaxTimestampPrecision() + ") WITH TIME ZONE";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return numeric ? getInt64Type() : getStringType(source, false, false);
    }

    @Override
    protected String getGeographyType() {
        return "SDO_GEOMETRY";
    }

    @Override
    protected String getGeometryType() {
        return "SDO_GEOMETRY";
    }

    @Override
    protected GeometryBytes getGeometryValues(ResultSet resultSet, int index) throws SQLException {
        final java.sql.Struct values = (java.sql.Struct) resultSet.getObject(index);
        final int srid = ((BigDecimal) values.getAttributes()[1]).intValue();

        final String query = "SELECT SDO_UTIL.TO_WKBGEOMETRY(TREAT(? AS SDO_GEOMETRY)) FROM DUAL";
        try (PreparedStatement statement = resultSet.getStatement().getConnection().prepareStatement(query)) {
            statement.setObject(1, values);
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    // Oracle's TO_WKBGEOMETRY returns EPSG format but in big endian format
                    final byte[] bytes = rs.getBytes(1);
                    return new GeometryBytes(bytes, srid).asLittleEndian();
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to read geometry value for index " + index, e);
        }

        throw new UnsupportedOperationException("Failed to read Oracle Geometry value");
    }
}

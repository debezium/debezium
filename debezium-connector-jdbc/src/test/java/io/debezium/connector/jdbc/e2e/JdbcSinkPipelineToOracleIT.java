/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;

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
    protected String getTimeType(Source source, boolean key, int precision) {
        return "DATE";
    }

    @Override
    protected String getTimeWithTimezoneType() {
        return "TIMESTAMP(6) WITH TIME ZONE";
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        if (source.getOptions().isColumnTypePropagated() && precision != 6 && !key) {
            return String.format("TIMESTAMP(%d)", precision);
        }
        return "TIMESTAMP(6)"; // DATE
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        if (source.getOptions().isColumnTypePropagated() && precision != 6 && !key) {
            return String.format("TIMESTAMP(%d) WITH TIME ZONE", precision);
        }
        return "TIMESTAMP(6) WITH TIME ZONE";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return numeric ? getInt64Type() : getStringType(source, false, false);
    }
}

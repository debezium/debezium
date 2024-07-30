/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.Db2SinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to Db2.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-db2")
@ExtendWith(Db2SinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToDb2IT extends AbstractJdbcSinkPipelineIT {

    @Override
    protected boolean isBitCoercedToBoolean() {
        return true;
    }

    @Override
    protected int getMaxDecimalPrecision() {
        return 31;
    }

    @Override
    protected String getBooleanType() {
        return "BOOLEAN";
    }

    @Override
    protected String getBitsDataType() {
        return "BLOB";
    }

    @Override
    protected String getInt8Type() {
        return "SMALLINT";
    }

    @Override
    protected String getInt16Type() {
        return "SMALLINT";
    }

    @Override
    protected String getInt32Type() {
        return "INTEGER";
    }

    @Override
    protected String getInt64Type() {
        return "BIGINT";
    }

    @Override
    protected String getVariableScaleDecimalType() {
        return "DOUBLE";
    }

    @Override
    protected String getDecimalType() {
        return "DECIMAL";
    }

    @Override
    protected String getFloat32Type() {
        return "REAL";
    }

    @Override
    protected String getFloat64Type() {
        return "DOUBLE";
    }

    @Override
    protected String getCharType(Source source, boolean key, boolean nationalized) {
        if (!source.getOptions().isColumnTypePropagated() && !key) {
            return "CLOB";
        }
        else if (!key) {
            return "CHAR";
        }
        return "VARCHAR";
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (maxLength) {
            return getTextType(nationalized);
        }
        if (!source.getOptions().isColumnTypePropagated() && !key) {
            return "CLOB";
        }
        return "VARCHAR";
    }

    @Override
    protected String getTextType(boolean nationalized) {
        return "CLOB";
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
        return key ? getStringType(source, key, false) : getTextType();
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        return key ? getStringType(source, key, false) : getTextType();
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
        return "TIME";
    }

    @Override
    protected String getTimeWithTimezoneType() {
        return "TIME";
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        return "TIMESTAMP";
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        return "TIMESTAMP";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return numeric ? getInt64Type() : getStringType(source, false, false);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.PostgresSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;

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
        return "TIMESTAMP";
    }

    @Override
    protected String getTimeWithTimezoneType() {
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
}

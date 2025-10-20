/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.junit.jupiter.OracleSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.doc.FixFor;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.transforms.ExtractNewRecordState;

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
        if (key) {
            return "TIMESTAMP(6)";
        }
        // Oracle only permits maximum of 6 digit precision
        return String.format("TIMESTAMP(%d)", Math.min(6, precision));
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

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.SQLSERVER, SourceType.POSTGRES }, reason = "No BLOB")
    @FixFor("DBZ-8276")
    public void testWritingBlob(Source source, Sink sink) throws Exception {
        final byte[] data = RandomStringUtils.randomAlphanumeric(256).getBytes(StandardCharsets.UTF_8);
        assertDataTypeNonKeyOnly(source,
                sink,
                "blob",
                (ps, index) -> {
                    final Blob blob = ps.getConnection().createBlob();
                    blob.setBytes(1, data);
                    ps.setBlob(index, blob);
                },
                // Oracle requires a key when writing LOB columns, hence BigDecimal
                List.of(BigDecimal.valueOf(1), data),
                (config) -> {
                },
                (properties) -> {
                    properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), PrimaryKeyMode.RECORD_KEY.getValue());
                    properties.put(JdbcSinkConnectorConfig.INSERT_MODE_FIELD.name(), InsertMode.UPSERT.getValue());
                },
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "BLOB")),
                (rs, index) -> index == 1 ? rs.getObject(index) : rs.getBytes(index));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.SQLSERVER, SourceType.POSTGRES }, reason = "No BLOB")
    @FixFor("DBZ-8276")
    public void testWritingBlobUsingExtractNewRecordStateTransform(Source source, Sink sink) throws Exception {
        final byte[] data = RandomStringUtils.randomAlphanumeric(256).getBytes(StandardCharsets.UTF_8);
        assertDataTypeNonKeyOnly(source,
                sink,
                "blob",
                (ps, index) -> {
                    final Blob blob = ps.getConnection().createBlob();
                    blob.setBytes(1, data);
                    ps.setBlob(index, blob);
                },
                // Oracle requires a key when writing LOB columns, hence BigDecimal
                List.of(BigDecimal.valueOf(1), data),
                (config) -> {
                    config.with("transforms", "unwrap");
                    config.with("transforms.unwrap.type", ExtractNewRecordState.class.getName());
                    config.with("transforms.unwrap.delete.tombstone.handling.mode", "rewrite-with-tombstone");
                },
                (properties) -> {
                    properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE_FIELD.name(), PrimaryKeyMode.RECORD_KEY.getValue());
                    properties.put(JdbcSinkConnectorConfig.INSERT_MODE_FIELD.name(), InsertMode.UPSERT.getValue());
                },
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "BLOB")),
                (rs, index) -> index == 1 ? rs.getObject(index) : rs.getBytes(index));
    }
}

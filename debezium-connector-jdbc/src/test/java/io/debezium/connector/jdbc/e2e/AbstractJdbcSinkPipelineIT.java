/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.InsertMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.PrimaryKeyMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode;
import io.debezium.connector.jdbc.junit.TestHelper;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkType;
import io.debezium.connector.jdbc.junit.jupiter.WithPostgresExtension;
import io.debezium.connector.jdbc.junit.jupiter.e2e.ForSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipExtractNewRecordState;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSource;
import io.debezium.connector.jdbc.junit.jupiter.e2e.WithTemporalPrecisionMode;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceConnectorOptions;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourcePipelineInvocationContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;
import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.time.MicroDuration;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

/**
 * An integration test class that holds all JDBC sink pipeline integration tests.
 *
 * <p>NOTE: It is important that all sink pipeline tests primarily exist in this class unless there
 * specific sink database tests that are in the derived sink class implementations to minimize
 * the resource start and tear down steps.
 *
 * <p>NOTE: All methods in this class should be annotated with {@link TestTemplate} as each method
 * will be invoked multiple times with various {@link Source} and {@link Sink} objects, which
 * allows the tests to verify that they successfully run with a variety of configurations.
 *
 * @author Chris Cranford
 * @see SourcePipelineInvocationContextProvider
 */
@ExtendWith(SourcePipelineInvocationContextProvider.class)
@SkipExtractNewRecordState
public abstract class AbstractJdbcSinkPipelineIT extends AbstractJdbcSinkIT {

    private final TableNamingStrategy tableNamingStrategy = new DefaultTableNamingStrategy();

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No BIT data type support")
    public void testBitDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bit",
                bitValues(source, "1", "0"),
                isBitCoercedToBoolean() ? List.of(true, false) : List.of(1, 0),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    switch (sink.getType()) {
                        case ORACLE:
                            assertColumn(sink, record, "id", getBooleanType(), 1);
                            assertColumn(sink, record, "data", getBooleanType(), 1);
                            break;
                        case POSTGRES:
                            assertColumn(sink, record, "id", getBooleanType());
                            assertColumn(sink, record, "data", options.isColumnTypePropagated() ? "BIT" : getBooleanType());
                            break;
                        default:
                            assertColumn(sink, record, "id", getBooleanType());
                            assertColumn(sink, record, "data", getBooleanType());
                            break;
                    }
                },
                (rs, index) -> isBitCoercedToBoolean() ? rs.getBoolean(index) : rs.getInt(index));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIT(n) data type support")
    @SkipWhenSink(value = { SinkType.ORACLE, SinkType.DB2 }, reason = "BIT(n) is sent as bytes, BLOB is not permitted in primary keys")
    public void testBitWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bit(2)",
                bitValues(source, "10", "01"),
                List.of(2, 1),
                (record) -> {
                    assertColumn(sink, record, "id", getBitsDataType(), 2);
                    assertColumn(sink, record, "data", getBitsDataType(), 2);
                },
                (rs, index) -> {
                    switch (sink.getType()) {
                        case POSTGRES:
                            return Integer.parseInt(rs.getString(index), 2);
                        case SQLSERVER:
                            return new BigInteger(rs.getBytes(index)).intValue();
                        default:
                            return rs.getInt(index);
                    }
                });
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIT(n) data type support")
    @SkipWhenSink(value = { SinkType.MYSQL, SinkType.POSTGRES, SinkType.SQLSERVER }, reason = "BIT(n) is only applicable to non-key columns")
    public void testBitWithSizeDataTypeNotInKey(Source source, Sink sink) throws Exception {
        final String tableName = source.randomTableName();
        registerSourceConnector(source, tableName);

        source.execute(String.format("CREATE TABLE %s (data bit(2))", tableName));
        source.streamTable(tableName);

        source.execute(String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", bitValues(source, "01"))));

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSink(source, sinkProperties, tableName);

        final SinkRecord record = consumeSinkRecord();
        assertColumn(sink, record, "data", getBitsDataType());

        sink.assertRows(getSinkTable(record, sink), rs -> {
            final Blob blob = rs.getBlob(1);
            assertThat(blob.getBytes(1, (int) blob.length())).isEqualTo(ByteBuffer.allocate(1).put((byte) 1).array());
            return null;
        });
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIT VARYING(n) data type support")
    @SkipWhenSink(value = { SinkType.ORACLE, SinkType.DB2 }, reason = "BIT VARYING(n) is sent as bytes, BLOB is not permitted in primary keys")
    public void testBitVaryingDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bit varying(2)",
                bitValues(source, "10", "01"),
                List.of(2, 1),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getBitsDataType(), 2);
                    if (options.isColumnTypePropagated() && sink.getType() == SinkType.POSTGRES) {
                        assertColumn(sink, record, "data", "VARBIT", 2);
                    }
                    else {
                        assertColumn(sink, record, "data", getBitsDataType(), 2);
                    }
                },
                (rs, index) -> {
                    switch (sink.getType()) {
                        case POSTGRES:
                            return Integer.parseInt(rs.getString(index), 2);
                        case SQLSERVER:
                            return new BigInteger(rs.getBytes(index)).intValue();
                        default:
                            return rs.getInt(index);
                    }
                });
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIT VARYING(n) data type support")
    @SkipWhenSink(value = { SinkType.MYSQL, SinkType.POSTGRES, SinkType.SQLSERVER }, reason = "BIT VARYING(n) is only applicable to non-key columns")
    public void testBitVaryingDataTypeNotInKey(Source source, Sink sink) throws Exception {
        final String tableName = source.randomTableName();
        registerSourceConnector(source, tableName);

        source.execute(String.format("CREATE TABLE %s (data bit varying(2))", tableName));
        source.streamTable(tableName);

        source.execute(String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", bitValues(source, "01"))));

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSink(source, sinkProperties, tableName);

        final SinkRecord record = consumeSinkRecord();
        assertColumn(sink, record, "data", getBitsDataType());

        sink.assertRows(getSinkTable(record, sink), rs -> {
            final Blob blob = rs.getBlob(1);
            assertThat(blob.getBytes(1, (int) blob.length())).isEqualTo(ByteBuffer.allocate(1).put((byte) 1).array());
            return null;
        });
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BOOLEAN data type support")
    public void testBooleanDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "boolean",
                List.of("true", "false"),
                List.of(1, 0),
                (config) -> applyJdbcSourceConverter(source, config, ".*id|.*data", null, null),
                (record) -> {
                    if (source.getType().is(SourceType.MYSQL)) {
                        // We explicitly use the JDBC source data type converter, maps BOOLEAN as INT16
                        assertColumn(sink, record, "id", getInt16Type());
                        if (sink.getType().is(SinkType.MYSQL) && source.getOptions().isColumnTypePropagated()) {
                            assertColumn(sink, record, "data", getBooleanType());
                        }
                        else {
                            assertColumn(sink, record, "data", getInt16Type());
                        }
                    }
                    else {
                        assertColumn(sink, record, "id", getBooleanType());
                        assertColumn(sink, record, "data", getBooleanType());
                    }
                },
                (rs, index) -> {
                    switch (sink.getType()) {
                        case POSTGRES:
                        case DB2:
                            return rs.getBoolean(index) ? 1 : 0;
                        default:
                            return rs.getInt(index);
                    }
                });
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE }, reason = "No TINYINT data type support")
    public void testTinyIntDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "tinyint",
                List.of(10, 12),
                (record) -> {
                    final boolean columnTypePropagated = source.getOptions().isColumnTypePropagated();
                    assertColumn(sink, record, "id", getInt16Type());
                    assertColumn(sink, record, "data", columnTypePropagated ? getInt8Type() : getInt16Type());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No TINYINT(n) data type support")
    public void testTinyIntWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "tinyint(2)",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    final boolean mysqlInt8 = SinkType.MYSQL.is(sink.getType()) && options.isColumnTypePropagated();
                    assertColumn(sink, record, "id", getInt16Type());
                    assertColumn(sink, record, "data", mysqlInt8 ? getInt8Type() : getInt16Type());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    public void testSmallIntDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "smallint",
                List.of(10, 12),
                (record) -> {
                    if (source.getType().is(SourceType.ORACLE)) {
                        // Oracle driver returns SMALLINT as NUMBER(38,0) and this forces the connector
                        // to emit the SMALLINT data type differently than other databases.
                        assertColumn(sink, record, "id", getDecimalType(), getMaxDecimalPrecision(), 0);
                        assertColumn(sink, record, "data", getDecimalType(), getMaxDecimalPrecision(), 0);
                    }
                    else {
                        assertColumn(sink, record, "id", getInt16Type());
                        assertColumn(sink, record, "data", getInt16Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No SMALLINT(n) data type support")
    public void testSmallIntWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "smallint(2)",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt16Type());
                    if (sink.getType().is(SinkType.ORACLE) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", getInt16Type(), 2);
                    }
                    else {
                        assertColumn(sink, record, "data", getInt16Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No SMALLSERIAL data type support")
    public void testSmallSerialDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "smallserial",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt16Type());
                    if (sink.getType().is(SinkType.POSTGRES) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "SMALLSERIAL");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt16Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No SERIAL data type support")
    public void testSerialDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "serial",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt32Type());
                    if (sink.getType().is(SinkType.POSTGRES) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "SERIAL");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt32Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIGSERIAL data type support")
    public void testBigSerialDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bigserial",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt64Type());
                    if (sink.getType().is(SinkType.POSTGRES) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "BIGSERIAL");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt64Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No MEDIUMINT data type support")
    public void testMediumIntDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "mediumint",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt32Type());
                    if (sink.getType().is(SinkType.MYSQL) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "MEDIUMINT");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt32Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No MEDIUMINT(n) data type support")
    public void testMediumIntWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "mediumint(2)",
                List.of(10, 12),
                (record) -> {
                    final SourceConnectorOptions options = source.getOptions();
                    assertColumn(sink, record, "id", getInt32Type());
                    if (sink.getType().is(SinkType.MYSQL) && options.isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "MEDIUMINT");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt32Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    public void testIntDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "int",
                List.of(10, 12),
                (record) -> {
                    if (source.getType().is(SourceType.ORACLE)) {
                        // Oracle driver returns SMALLINT as NUMBER(38,0) and this forces the connector
                        // to emit the SMALLINT data type differently than other databases.
                        assertColumn(sink, record, "id", getDecimalType(), getMaxDecimalPrecision(), 0);
                        assertColumn(sink, record, "data", getDecimalType(), getMaxDecimalPrecision(), 0);
                    }
                    else {
                        assertColumn(sink, record, "id", getInt32Type());
                        assertColumn(sink, record, "data", getInt32Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    public void testIntegerDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "integer",
                List.of(10, 12),
                (record) -> {
                    if (source.getType().is(SourceType.ORACLE)) {
                        // Oracle driver returns SMALLINT as NUMBER(38,0) and this forces the connector
                        // to emit the SMALLINT data type differently than other databases.
                        assertColumn(sink, record, "id", getDecimalType(), getMaxDecimalPrecision(), 0);
                        assertColumn(sink, record, "data", getDecimalType(), getMaxDecimalPrecision(), 0);
                    }
                    else {
                        assertColumn(sink, record, "id", getInt32Type());
                        assertColumn(sink, record, "data", getInt32Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No INTEGER(n) data type support")
    public void testIntegerWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "integer(2)",
                List.of(10, 12),
                (record) -> {
                    assertColumn(sink, record, "id", getInt32Type());
                    assertColumn(sink, record, "data", getInt32Type());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No BIGINT data type support")
    public void testBigIntDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bigint",
                List.of(10, 12),
                (record) -> {
                    assertColumn(sink, record, "id", getInt64Type());
                    assertColumn(sink, record, "data", getInt64Type());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BIGINT(n) data type support")
    public void testBigIntWithSizeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bigint(2)",
                List.of(10, 12),
                (record) -> {
                    assertColumn(sink, record, "id", getInt64Type());
                    assertColumn(sink, record, "data", getInt64Type());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NUMBER data type support")
    public void testNumberDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "number",
                List.of(10, 12),
                (record) -> {
                    assertColumn(sink, record, "id", getVariableScaleDecimalType());
                    assertColumn(sink, record, "data", getVariableScaleDecimalType());
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NUMBER(n) data type support")
    public void testNumberWithPrecisionDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("number(2)", "number(3)", "number(8)", "number(18)", "number(24)"),
                List.of(10, 12, 14, 16, 18),
                (record) -> {
                    assertColumn(sink, record, "id0", getInt8Type());
                    assertColumn(sink, record, "id1", getInt16Type());
                    assertColumn(sink, record, "id2", getInt32Type());
                    assertColumn(sink, record, "id3", getInt64Type());
                    assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                    assertColumn(sink, record, "data0", getInt8Type());
                    assertColumn(sink, record, "data1", getInt16Type());
                    assertColumn(sink, record, "data2", getInt32Type());
                    assertColumn(sink, record, "data3", getInt64Type());
                    assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NUMBER(n,s) data type support")
    public void testNumberWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("number(2,1)", "number(3,1)", "number(8,1)", "number(18,1)", "number(24,1)"),
                List.of(1.d, 10.d, 11.d, 12.d, 13.d),
                (record) -> {
                    assertColumn(sink, record, "id0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "id1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "id2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "id3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "id4", getDecimalType(), 24, 1);
                    assertColumn(sink, record, "data0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "data1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "data2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "data3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "data4", getDecimalType(), 24, 1);
                },
                ResultSet::getDouble);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NUMBER(n,s) negative scale data type support")
    public void testNumberWithPrecisionAndNegativeScaleDataType(Source source, Sink sink) throws Exception {
        // NOTE:
        // Oracle supports negative scale data types, where this acts as a way to round the column value
        // based on the supplied scale. For example, a scale of -1 rounds to a factor of 10 while scale
        // of -3 rounds to factor of 1000.
        //
        // Since Oracle rounds the values on the source side when this is used, the values passed into a
        // Kafka event are also truncated, and therefore, it's safe to just write the value into the sink
        // column as is. If the sink database is Oracle, the negative scale will be applied to the column
        // definition if column type propagation is enabled; otherwise a scale of 0 will be assumed.
        assertDataTypes(source,
                sink,
                List.of("number(2,-1)", "number(3,-1)", "number(8,-1)", "number(18,-1)", "number(24,-3)"),
                List.of(1L, 111L, 11111111L, 111111111111111111L, 111111111111111111L),
                List.of(0L, 110L, 11111110L, 111111111111111110L, 111111111111111000L),
                (record) -> {
                    // The Kafka Connect integer types do not propagate length, precision/scale to
                    // the sink as these are not necessarily universal, which explains the variance
                    // for types with precision < 9. For precision >= 9, types are emitted as Decimal
                    // does have length, precision/scale and negative scale values. The negative
                    // scale values are only applied if the sink supports such values.
                    final boolean mysqlSink = sink.getType().is(SinkType.MYSQL);
                    assertColumn(sink, record, "id0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "id1", getInt16Type());
                    assertColumn(sink, record, "id2", getInt32Type());
                    assertColumn(sink, record, "data0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "data1", getInt16Type());
                    assertColumn(sink, record, "data2", getInt32Type());
                    if (SinkType.ORACLE.is(sink.getType())) {
                        assertColumn(sink, record, "id3", getDecimalType(), 18, -1);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, -3);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, -1);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, -3);
                    }
                    else {
                        assertColumn(sink, record, "id3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                },
                ResultSet::getLong);
    }

    @TestTemplate
    public void testNumericDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "numeric",
                List.of(10, 12),
                (record) -> {
                    // Precision assertion is skipped, varies by the source.
                    if (source.getType().is(SourceType.POSTGRES)) {
                        assertColumn(sink, record, "id", getVariableScaleDecimalType());
                        assertColumn(sink, record, "data", getVariableScaleDecimalType());
                    }
                    else {
                        assertColumn(sink, record, "id", getDecimalType());
                        assertColumn(sink, record, "data", getDecimalType());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    public void testNumericWithPrecisionDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("numeric(2)", "numeric(3)", "numeric(8)", "numeric(18)", "numeric(24)"),
                List.of(10L, 11L, 12L, 13L, 14L),
                (record) -> {
                    // todo: should we align Oracle with other connectors?
                    // Other sources emit these data types as Decimal (BYTES) even for the lower
                    // precisions; however, Oracle does not do this and instead elects to try
                    // and pick the best Connect equivalent (INT 8/16/32/64) based on the column
                    // precision and emits the value as those types, reserving the use of the
                    // Decimal (BYTES) style for precision >= 19.
                    if (SourceType.ORACLE.is(source.getType())) {
                        assertColumn(sink, record, "id0", getInt8Type());
                        assertColumn(sink, record, "id1", getInt16Type());
                        assertColumn(sink, record, "id2", getInt32Type());
                        assertColumn(sink, record, "id3", getInt64Type());
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data0", getInt8Type());
                        assertColumn(sink, record, "data1", getInt16Type());
                        assertColumn(sink, record, "data2", getInt32Type());
                        assertColumn(sink, record, "data3", getInt64Type());
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                    else {
                        assertColumn(sink, record, "id0", getDecimalType(), 2, 0);
                        assertColumn(sink, record, "id1", getDecimalType(), 3, 0);
                        assertColumn(sink, record, "id2", getDecimalType(), 8, 0);
                        assertColumn(sink, record, "id3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data0", getDecimalType(), 2, 0);
                        assertColumn(sink, record, "data1", getDecimalType(), 3, 0);
                        assertColumn(sink, record, "data2", getDecimalType(), 8, 0);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                },
                ResultSet::getLong);
    }

    @TestTemplate
    public void testNumericWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("numeric(2,1)", "numeric(3,1)", "numeric(8,1)", "numeric(18,1)", "numeric(24,1)"),
                List.of(1.d, 10.d, 11.d, 12.d, 13.d),
                (record) -> {
                    assertColumn(sink, record, "id0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "id1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "id2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "id3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "id4", getDecimalType(), 24, 1);
                    assertColumn(sink, record, "data0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "data1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "data2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "data3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "data4", getDecimalType(), 24, 1);
                },
                ResultSet::getDouble);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NUMERIC(n,s) negative scale data type support")
    public void testNumericWithPrecisionAndNegativeScaleDataType(Source source, Sink sink) throws Exception {
        // NOTE:
        // Oracle supports negative scale data types, where this acts as a way to round the column value
        // based on the supplied scale. For example, a scale of -1 rounds to a factor of 10 while scale
        // of -3 rounds to factor of 1000.
        //
        // Since Oracle rounds the values on the source side when this is used, the values passed into a
        // Kafka event are also truncated, and therefore, it's safe to just write the value into the sink
        // column as is. If the sink database is Oracle, the negative scale will be applied to the column
        // definition if column type propagation is enabled; otherwise a scale of 0 will be assumed.
        assertDataTypes(source,
                sink,
                List.of("numeric(2,-1)", "numeric(3,-1)", "numeric(8,-1)", "numeric(18,-1)", "numeric(24,-3)"),
                List.of(1L, 111L, 11111111L, 111111111111111111L, 111111111111111111L),
                List.of(0L, 110L, 11111110L, 111111111111111110L, 111111111111111000L),
                (record) -> {
                    // The Kafka Connect integer types do not propagate length, precision/scale to
                    // the sink as these are not necessarily universal, which explains the variance
                    // for types with precision < 9. For precision >= 9, types are emitted as Decimal
                    // does have length, precision/scale and negative scale values. The negative
                    // scale values are only applied if the sink supports such values.
                    final boolean oracleSink = sink.getType().is(SinkType.ORACLE);
                    final boolean mysqlSink = sink.getType().is(SinkType.MYSQL);
                    assertColumn(sink, record, "id0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "id1", getInt16Type());
                    assertColumn(sink, record, "id2", getInt32Type());
                    assertColumn(sink, record, "id3", getDecimalType(), 18, oracleSink ? -1 : 0);
                    assertColumn(sink, record, "id4", getDecimalType(), 24, oracleSink ? -3 : 0);
                    assertColumn(sink, record, "data0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "data1", getInt16Type());
                    assertColumn(sink, record, "data2", getInt32Type());
                    assertColumn(sink, record, "data3", getDecimalType(), 18, oracleSink ? -1 : 0);
                    assertColumn(sink, record, "data4", getDecimalType(), 24, oracleSink ? -3 : 0);
                },
                ResultSet::getLong);
    }

    @TestTemplate
    public void testDecimalDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "decimal",
                List.of(10, 12),
                (record) -> {
                    // Precision assertion is skipped, varies by the source.
                    if (source.getType().is(SourceType.POSTGRES)) {
                        assertColumn(sink, record, "id", getVariableScaleDecimalType());
                        assertColumn(sink, record, "data", getVariableScaleDecimalType());
                    }
                    else {
                        assertColumn(sink, record, "id", getDecimalType());
                        assertColumn(sink, record, "data", getDecimalType());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    public void testDecimalWithPrecisionDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("decimal(2)", "decimal(3)", "decimal(8)", "decimal(18)", "decimal(24)"),
                List.of(10L, 11L, 12L, 13L, 14L),
                (record) -> {
                    // todo: should we align Oracle with other connectors?
                    // Just like the Number(p) test method above, other sources emit these data
                    // types as Decimal (BYTES) even for the lower precisions; however, Oracle
                    // does not do this and instead elects to try and pick the best Connect
                    // equivalent (INT 8/16/32/64) based on the column precision an demits the
                    // value as those types, reserving the use of the Decimal (BYTES) style
                    // for precision >= 19.
                    if (SourceType.ORACLE.is(source.getType())) {
                        assertColumn(sink, record, "id0", getInt8Type());
                        assertColumn(sink, record, "id1", getInt16Type());
                        assertColumn(sink, record, "id2", getInt32Type());
                        assertColumn(sink, record, "id3", getInt64Type());
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data0", getInt8Type());
                        assertColumn(sink, record, "data1", getInt16Type());
                        assertColumn(sink, record, "data2", getInt32Type());
                        assertColumn(sink, record, "data3", getInt64Type());
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                    else {
                        assertColumn(sink, record, "id0", getDecimalType(), 2, 0);
                        assertColumn(sink, record, "id1", getDecimalType(), 3, 0);
                        assertColumn(sink, record, "id2", getDecimalType(), 8, 0);
                        assertColumn(sink, record, "id3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data0", getDecimalType(), 2, 0);
                        assertColumn(sink, record, "data1", getDecimalType(), 3, 0);
                        assertColumn(sink, record, "data2", getDecimalType(), 8, 0);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                },
                ResultSet::getLong);
    }

    @TestTemplate
    public void testDecimalWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataTypes(source,
                sink,
                List.of("decimal(2,1)", "decimal(3,1)", "decimal(8,1)", "decimal(18,1)", "decimal(24,1)"),
                List.of(1.d, 10.d, 11.d, 12.d, 13.d),
                (record) -> {
                    assertColumn(sink, record, "id0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "id1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "id2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "id3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "id4", getDecimalType(), 24, 1);
                    assertColumn(sink, record, "data0", getDecimalType(), 2, 1);
                    assertColumn(sink, record, "data1", getDecimalType(), 3, 1);
                    assertColumn(sink, record, "data2", getDecimalType(), 8, 1);
                    assertColumn(sink, record, "data3", getDecimalType(), 18, 1);
                    assertColumn(sink, record, "data4", getDecimalType(), 24, 1);
                },
                ResultSet::getDouble);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No DECIMAL(n,s) negative scale data type support")
    public void testDecimalWithPrecisionAndNegativeScaleDataType(Source source, Sink sink) throws Exception {
        // NOTE:
        // Oracle supports negative scale data types, where this acts as a way to round the column value
        // based on the supplied scale. For example, a scale of -1 rounds to a factor of 10 while scale
        // of -3 rounds to factor of 1000.
        //
        // Since Oracle rounds the values on the source side when this is used, the values passed into a
        // Kafka event are also truncated, and therefore, it's safe to just write the value into the sink
        // column as is. If the sink database is Oracle, the negative scale will be applied to the column
        // definition if column type propagation is enabled; otherwise a scale of 0 will be assumed.
        assertDataTypes(source,
                sink,
                List.of("decimal(2,-1)", "decimal(3,-1)", "decimal(8,-1)", "decimal(18,-1)", "decimal(24,-3)"),
                List.of(1L, 111L, 11111111L, 111111111111111111L, 111111111111111111L),
                List.of(0L, 110L, 11111110L, 111111111111111110L, 111111111111111000L),
                (record) -> {
                    // The Kafka Connect integer types do not propagate length, precision/scale to
                    // the sink as these are not necessarily universal, which explains the variance
                    // for types with precision < 9. For precision >= 9, types are emitted as Decimal
                    // does have length, precision/scale and negative scale values. The negative
                    // scale values are only applied if the sink supports such values.
                    final boolean mysqlSink = sink.getType().is(SinkType.MYSQL);
                    assertColumn(sink, record, "id0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "id1", getInt16Type());
                    assertColumn(sink, record, "id2", getInt32Type());
                    assertColumn(sink, record, "data0", mysqlSink ? getInt16Type() : getInt8Type());
                    assertColumn(sink, record, "data1", getInt16Type());
                    assertColumn(sink, record, "data2", getInt32Type());
                    if (SinkType.ORACLE.is(sink.getType())) {
                        assertColumn(sink, record, "id3", getDecimalType(), 18, -1);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, -3);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, -1);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, -3);
                    }
                    else {
                        assertColumn(sink, record, "id3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "id4", getDecimalType(), 24, 0);
                        assertColumn(sink, record, "data3", getDecimalType(), 18, 0);
                        assertColumn(sink, record, "data4", getDecimalType(), 24, 0);
                    }
                },
                ResultSet::getLong);
    }

    @TestTemplate
    public void testRealDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "real",
                List.of(3.14f, 3.14f),
                (config) -> applyJdbcSourceConverter(source, config, null, ".*id|.*data", null),
                (record) -> {
                    // Oracle emits VariableScaleDecimal which maps to DOUBLE types
                    final String expectedType = source.getType().is(SourceType.ORACLE, SourceType.MYSQL)
                            ? getFloat64Type()
                            : getFloat32Type();

                    assertColumn(sink, record, "id", expectedType);
                    assertColumn(sink, record, "data", expectedType);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "Applies to MySQL JDBC custom converter")
    public void testRealDataTypeTreatAsFloat(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "real",
                List.of(3.14f, 3.14f),
                (config) -> {
                    applyJdbcSourceConverter(source, config, null, ".*id|.*data", null);
                    // By default, the custom converter treats reals as doubles to align with the
                    // default MySQL handling of the REAL data type. When set to "false", this
                    // forces MySQL to emit REAL data types as FLOAT/FLOAT32.
                    config.with("jdbc-sink.treat.real.as.double", "false");
                },
                (record) -> {
                    // Oracle emits VariableScaleDecimal which maps to DOUBLE types
                    final String expectedType = source.getType().is(SourceType.ORACLE)
                            ? getFloat64Type()
                            : getFloat32Type();

                    assertColumn(sink, record, "id", expectedType);
                    assertColumn(sink, record, "data", expectedType);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @Disabled("Not supported by any of our current source connectors")
    @SuppressWarnings("unused")
    public void testRealWithPrecisionDataType(Source source, Sink sink) throws Exception {
        throw new IllegalStateException("Not yet implemented");
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No REAL(p,s) data type support")
    public void testRealWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "real(4, 2)",
                List.of(3.14f, 3.14f),
                (config) -> applyJdbcSourceConverter(source, config, null, ".*id|.*data", null),
                (record) -> {
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    public void testFloatDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "float",
                List.of(3.14f, 3.14f),
                (record) -> {
                    // Oracle emits VariableScaleDecimal which maps to DOUBLE types
                    final String expectedType = !source.getType().is(SourceType.MYSQL) ? getFloat64Type() : getFloat32Type();
                    assertColumn(sink, record, "id", expectedType);
                    assertColumn(sink, record, "data", expectedType);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    public void testFloatWithPrecisionDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "float(8)",
                List.of(3.14f, 3.14f),
                (record) -> {
                    // Oracle emits VariableScaleDecimal which maps to DOUBLE types
                    final String expectedType = source.getType().is(SourceType.ORACLE) ? getFloat64Type() : getFloat32Type();
                    assertColumn(sink, record, "id", expectedType);
                    assertColumn(sink, record, "data", expectedType);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No FLOAT(p,s) data type support")
    public void testFloatWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "float(4, 2)",
                List.of(3.14f, 3.14f),
                (record) -> {
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No DOUBLE data type support")
    public void testDoubleDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "double",
                List.of(3.14f, 3.14f),
                (record) -> {
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @Disabled("Not supported by any of our currently supported source databases")
    @SuppressWarnings("unused")
    public void testDoubleWithPrecisionDataType(Source source, Sink sink) throws Exception {
        throw new IllegalStateException("Not yet implemented");
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No DOUBLE(p,s) data type support")
    public void testDoubleWithPrecisionAndScaleDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "double(4, 2)",
                List.of(3.14f, 3.14f),
                (record) -> {
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    public void testDoublePrecisionDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "double precision",
                List.of(3.14d, 3.14d),
                (record) -> {
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getDouble);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No BINARY_FLOAT data type support")
    public void testBinaryFloatDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "binary_float",
                List.of(3.14f, 3.14f),
                (record) -> {
                    // Assumes sink default precision and scale.
                    assertColumn(sink, record, "id", getFloat32Type());
                    assertColumn(sink, record, "data", getFloat32Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No BINARY_DOUBLE data type support")
    public void testBinaryDoubleDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "binary_double",
                List.of(3.14f, 3.14f),
                (record) -> {
                    // Assumes sink default precision and scale.
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No SMALLMONEY data type support")
    public void testSmallMoneyDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "smallmoney",
                List.of(3.14f, 3.14f),
                (record) -> {
                    assertColumn(sink, record, "id", getDecimalType(), 10, 4);
                    assertColumn(sink, record, "data", getDecimalType(), 10, 4);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No MONEY data type support")
    public void testMoneyDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "money",
                List.of(3.14f, 3.14f),
                (record) -> {
                    assertColumn(sink, record, "id", getDecimalType(), 19, 4);
                    assertColumn(sink, record, "data", getDecimalType(), 19, 4);
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    public void testDecimalHandlingModeDouble(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "decimal",
                List.of(10.0f, 12.0f),
                (config) -> config.with("decimal.handling.mode", DecimalHandlingMode.DOUBLE.getValue()),
                (record) -> {
                    // DecimalHandlingMode always dispatches with values as FLOAT64 data types
                    // This can be asserted here for every data-type as the resolution should be identical
                    // since the sink bases the resolution on the FLOAT64 schema type.
                    assertColumn(sink, record, "id", getFloat64Type());
                    assertColumn(sink, record, "data", getFloat64Type());
                },
                ResultSet::getFloat);
    }

    @TestTemplate
    public void testDecimalHandlingModeString(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "decimal",
                List.of(10.0f, 12.0f),
                source.getType() == SourceType.POSTGRES ? List.of("10.0", "12.0") : List.of("10", "12"),
                (config) -> config.with("decimal.handling.mode", DecimalHandlingMode.STRING.getValue()),
                (record) -> {
                    // DecimalHandlingMode always dispatches with values as STRING data types
                    // When column propagation is enabled, original data types, scale, and precision are
                    // provided for non-key columns; but for now we're going to simply map the columns
                    // to their STRING mapping equivalents.
                    switch (sink.getType()) {
                        case MYSQL:
                            assertColumn(sink, record, "id", "VARCHAR", 255);
                            assertColumn(sink, record, "data", "LONGTEXT", Integer.MAX_VALUE);
                            break;
                        case DB2:
                            assertColumn(sink, record, "id", "VARCHAR", 512);
                            assertColumn(sink, record, "data", "CLOB");
                            break;
                        case ORACLE:
                            assertColumn(sink, record, "id", "VARCHAR2", 4000);
                            assertColumn(sink, record, "data", "CLOB");
                            break;
                        case SQLSERVER:
                            assertColumn(sink, record, "id", "VARCHAR", 900);
                            assertColumn(sink, record, "data", "VARCHAR", Integer.MAX_VALUE);
                            break;
                        case POSTGRES:
                            assertColumn(sink, record, "id", "TEXT");
                            assertColumn(sink, record, "data", "TEXT");
                            break;
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    public void testCharDataType(Source source, Sink sink) throws Exception {
        assertCharDataType(source, sink, "char", false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "Awaiting the merging of DBZ-6221 upstream")
    public void testCharacterDataType(Source source, Sink sink) throws Exception {
        assertCharDataType(source, sink, "character", false);
    }

    @TestTemplate
    public void testCharWithLengthDataType(Source source, Sink sink) throws Exception {
        assertCharWithLengthDataType(source, sink, "char(5)", 5, false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "Awaiting the merging of DBZ-6221 upstream")
    public void testCharacterWithLengthDataType(Source source, Sink sink) throws Exception {
        assertCharWithLengthDataType(source, sink, "character(5)", 5, false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES }, reason = "NCHAR is treated as CHAR as PostgreSQL does not use nationalized types")
    public void testNationalizedCharDataType(Source source, Sink sink) throws Exception {
        assertCharDataType(source, sink, "nchar", true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES }, reason = "NCHARACTER is treated as CHAR as PostgreSQL does not use nationalized types")
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE }, reason = "NCHARACTER not a supported data type")
    public void testNationalizedCharacterDataType(Source source, Sink sink) throws Exception {
        assertCharDataType(source, sink, "ncharacter", true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES }, reason = "NCHAR(n) is treated as CHAR(n) as PostgreSQL does not use nationalized types")
    public void testNationalizedCharWithLengthDataType(Source source, Sink sink) throws Exception {
        assertCharWithLengthDataType(source, sink, "nchar(5)", 5, true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES }, reason = "NCHAR(n) is treated as CHAR(n) as PostgreSQL does not use nationalized types")
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE }, reason = "NCHARACTER(n) not a supported data type")
    public void testNationalizedCharacterWithLengthDataType(Source source, Sink sink) throws Exception {
        assertCharWithLengthDataType(source, sink, "ncharacter(5)", 5, true);
    }

    @TestTemplate
    public void testVarcharDataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "varchar(25)", 25, false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No VARCHAR2(n) data type support")
    public void testVarchar2DataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "varchar2(25)", 25, false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE }, reason = "No NVARCHAR(n) data type support")
    public void testNVarcharDataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "nvarchar(25)", 25, true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NVARCHAR2(n) data type support")
    public void testNVarchar2DataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "nvarchar2(25)", 25, true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "Awaiting the merging of DBZ-6221 upstream")
    public void testCharacterVaryingDataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "character varying(25)", 25, false);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No NCHARACTER VARYING(n) data type support")
    public void testNCharacterVaryingDataType(Source source, Sink sink) throws Exception {
        assertVarcharDataType(source, sink, "ncharacter varying(25)", 25, true);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No TINYTEXT data type support")
    public void testTinyTextDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "tinytext",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No MEDIUMTEXT data type support")
    public void testMediumTextDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "mediumtext",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No MEDIUMTEXT data type support")
    public void testLongTextDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "longtext",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No TEXT data type support")
    public void testTextDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "text",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No NTEXT data type support")
    public void testNTextDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "ntext",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No CLOB data type support")
    public void testClobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "clob",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No NCLOB data type support")
    public void testNClobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "nclob",
                List.of("'hello world'"),
                List.of("hello world"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE }, reason = "No BINARY(n) data type support")
    public void testBinaryDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "binary(15)",
                List.of(binaryValue(source, "binary(15)", "'hello world'")),
                List.of(byteArrayPadded("hello world", 15)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "binary")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE }, reason = "No VARBINARY(n) data type support")
    public void testVarBinaryDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "varbinary(15)",
                List.of(binaryValue(source, "varbinary(15)", "'hello world'")),
                List.of("hello world".getBytes(StandardCharsets.UTF_8)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "varbinary")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No TINYBLOB data type support")
    public void testTinyBlobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "tinyblob",
                List.of("'hello world'"),
                List.of("hello world".getBytes(StandardCharsets.UTF_8)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "tinyblob")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No MEDIUMBLOB data type support")
    public void testMediumBlobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "mediumblob",
                List.of("'hello world'"),
                List.of("hello world".getBytes(StandardCharsets.UTF_8)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "mediumblob")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No LONGBLOB data type support")
    public void testLongBlobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "longblob",
                List.of("'hello world'"),
                List.of("hello world".getBytes(StandardCharsets.UTF_8)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "longblob")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No BLOB data type support")
    public void testBlobDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "blob",
                List.of(source.getType().is(SourceType.ORACLE) ? "UTL_RAW.CAST_TO_RAW('hello world')" : "'hello world'"),
                List.of("hello world".getBytes(StandardCharsets.UTF_8)),
                (record) -> assertColumn(sink, record, "data", getBinaryType(source, "blob")),
                ResultSet::getBytes);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "Only a single source is needed")
    public void testBinaryHandlingModeBase64(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "varbinary(35)",
                List.of(binaryValue(source, "varbinary(35)", "'hello world'")),
                List.of(Base64.getEncoder().encodeToString("hello world".getBytes(StandardCharsets.UTF_8))),
                (config) -> config.with("binary.handling.mode", BinaryHandlingMode.BASE64.getValue()),
                (record) -> assertColumn(sink, record, "data", getStringType(source, false, false, true)),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "Only a single source is needed")
    public void testBinaryHandlingModeBase64UrlSafe(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "varbinary(35)",
                List.of(binaryValue(source, "varbinary(35)", "'hello world'")),
                List.of(Base64.getUrlEncoder().encodeToString("hello world".getBytes(StandardCharsets.UTF_8))),
                (config) -> config.with("binary.handling.mode", BinaryHandlingMode.BASE64_URL_SAFE.getValue()),
                (record) -> assertColumn(sink, record, "data", getStringType(source, false, false, true)),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "Only a single source is needed")
    public void testBinaryHandlingModeHex(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "varbinary(35)",
                List.of(binaryValue(source, "varbinary(35)", "'hello world'")),
                List.of(HexConverter.convertToHexString("hello world".getBytes(StandardCharsets.UTF_8))),
                (config) -> config.with("binary.handling.mode", BinaryHandlingMode.HEX.getValue()),
                (record) -> assertColumn(sink, record, "data", getStringType(source, false, false, true)),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No JSON data type support")
    public void testJsonDataType(Source source, Sink sink) throws Exception {
        final String json = "{\"key\": \"value\"}";
        assertDataTypeNonKeyOnly(source,
                sink,
                "json",
                List.of(String.format("'%s'", json)),
                List.of(new ObjectMapper().readTree(json)),
                (record) -> assertColumn(sink, record, "data", getJsonType(source)),
                (rs, index) -> new ObjectMapper().readTree(rs.getString(index)));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No JSONB data type support")
    public void testJsonbDataType(Source source, Sink sink) throws Exception {
        final String json = "{\"key\": \"value\"}";
        assertDataTypeNonKeyOnly(source,
                sink,
                "jsonb",
                List.of(String.format("'%s'", json)),
                List.of(new ObjectMapper().readTree(json)),
                (record) -> assertColumn(sink, record, "data", getJsonbType(source)),
                (rs, index) -> new ObjectMapper().readTree(rs.getString(index)));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE }, reason = "No XML data type support")
    public void testXmlDataType(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "xml",
                List.of("'<doc>abc</doc>'"),
                List.of("<doc>abc</doc>"),
                (record) -> assertColumn(sink, record, "data", getXmlType(source)),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No UUID data type support")
    public void testUuidDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "uuid",
                List.of("'77412aae-c023-11ed-afa1-0242ac120002'", "'ed338923-f8ac-404c-87e7-e1ba5a122a12'"),
                List.of("77412aae-c023-11ed-afa1-0242ac120002", "ed338923-f8ac-404c-87e7-e1ba5a122a12"),
                (record) -> {
                    assertColumn(sink, record, "id", getUuidType(source, true));
                    assertColumn(sink, record, "data", getUuidType(source, false));
                },
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No ENUM data type support")
    public void testEnumDataType(Source source, Sink sink) throws Exception {
        // Create enum data type as needed.
        final String enumDataType;
        if (SourceType.POSTGRES.is(source.getType())) {
            enumDataType = source.randomObjectName();
            source.execute(String.format("CREATE TYPE %s as ENUM ('apples', 'oranges')", enumDataType));
        }
        else {
            // MySQL uses this format
            enumDataType = "enum('apples', 'oranges')";
        }

        assertDataType(source,
                sink,
                enumDataType,
                List.of("'apples'", "'oranges'"),
                List.of("apples", "oranges"),
                (record) -> {
                    assertColumn(sink, record, "id", getEnumType(source, true));
                    assertColumn(sink, record, "data", getEnumType(source, false));
                },
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No SET data type support")
    public void testSetDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "set('apples','oranges')",
                List.of("'apples'", "'oranges'"),
                List.of("apples", "oranges"),
                (record) -> {
                    assertColumn(sink, record, "id", getSetType(source, true));
                    assertColumn(sink, record, "data", getSetType(source, false));
                },
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No SET data type support")
    public void testYearDataType(Source source, Sink sink) throws Exception {
        // YEAR(2) support was removed in MySQL 8; expects using YEAR or YEAR(4); which are synonymous.
        assertDataType(source,
                sink,
                "year",
                List.of(1969, 2023),
                (record) -> {
                    if (SinkType.MYSQL.is(sink.getType())) {
                        assertColumn(sink, record, "id", getYearType(), 4);
                        assertColumn(sink, record, "data", getYearType(), 4);
                    }
                    else {
                        assertColumn(sink, record, "id", getYearType());
                        assertColumn(sink, record, "data", getYearType());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @WithTemporalPrecisionMode
    public void testDateDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "date",
                List.of(dateValue(source, 3, 1, 2023), dateValue(source, 12, 31, 2021)),
                List.of(Date.valueOf("2023-03-01"), Date.valueOf("2021-12-31")),
                (record) -> {
                    // todo: Should this behavior be aligned?
                    // While Oracle returns the JDBC type name as "DATE", it explicitly maps the sql type
                    // code to Types.TIMESTAMP, which means that the connector emits this column as a
                    // TIMESTAMP where-as other databases return the sql type code as Types.DATE and the
                    // values are serialized as such.
                    if (SourceType.ORACLE.is(source.getType())) {
                        assertColumn(sink, record, "id", getTimestampType(source, true, 6));
                        assertColumn(sink, record, "data", getTimestampType(source, false, 6));
                    }
                    else {
                        assertColumn(sink, record, "id", getDateType());
                        assertColumn(sink, record, "data", getDateType());
                    }
                },
                ResultSet::getDate);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No TIME data type support")
    @WithTemporalPrecisionMode
    public void testTimeDataType(Source source, Sink sink) throws Exception {
        final boolean connect = source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT;
        int nanoSeconds = connect ? 123000000 : 123456000;
        switch (source.getType()) {
            case MYSQL:
                // Emitted as seconds precision.
                nanoSeconds = 0;
                break;
            case SQLSERVER:
                if (!connect) {
                    // todo: See DBZ-6222
                    // SQL Server currently emits time-based precision values differently between snapshot/streaming.
                    // During snapshot, they're emitted with microsecond precision, streaming uses milliseconds.
                    nanoSeconds = source.getOptions().useSnapshot() ? 123456000 : 123000000;
                }
                break;
        }

        assertDataType(source,
                sink,
                "time",
                List.of("'01:02:03.123456'", "'14:15:16.123456'"),
                List.of(OffsetDateTime.of(1970, 1, 1, 1, 2, 3, nanoSeconds, ZoneOffset.UTC),
                        OffsetDateTime.of(1970, 1, 1, 14, 15, 16, nanoSeconds, ZoneOffset.UTC)),
                (record) -> {
                    assertColumn(sink, record, "id", getTimeType(source, true, 6));
                    assertColumn(sink, record, "data", getTimeType(source, false, 6));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No TIME(n) data type support")
    @WithTemporalPrecisionMode
    public void testTimeWithPrecisionDataType(Source source, Sink sink) throws Exception {
        final String ts0 = "'01:02:03.123456'";
        final String ts1 = "'14:15:16.456789'";

        final List<OffsetTime> expectedValues;
        if (SourceType.SQLSERVER.is(source.getType())) {
            final boolean connect = source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT;
            if (source.getOptions().useSnapshot()) {
                // todo: See DBZ-6222
                // SQL Server rounds TIME(n) to the nearest millisecond during snapshot, see second value
                int nanoSeconds = connect ? 456000000 : 456789000;
                expectedValues = List.of(OffsetTime.of(1, 2, 3, 123000000, ZoneOffset.UTC),
                        OffsetTime.of(14, 15, 16, nanoSeconds, ZoneOffset.UTC));
            }
            else {
                // SQL Server rounds TIME(n) to the nearest microsecond during streaming, see second value
                expectedValues = List.of(OffsetTime.of(1, 2, 3, 123000000, ZoneOffset.UTC),
                        OffsetTime.of(14, 15, 16, 457000000, ZoneOffset.UTC));
            }
        }
        else if (source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT) {
            // There is always a loss of precision on time(p) where p > 3 using connect precision
            expectedValues = List.of(OffsetTime.of(1, 2, 3, 123000000, ZoneOffset.UTC),
                    OffsetTime.of(14, 15, 16, 456000000, ZoneOffset.UTC));
        }
        else {
            expectedValues = List.of(OffsetTime.of(1, 2, 3, 123000000, ZoneOffset.UTC),
                    OffsetTime.of(14, 15, 16, 456789000, ZoneOffset.UTC));
        }

        assertDataTypes(source,
                sink,
                List.of("time(3)", "time(6)"),
                List.of(ts0, ts1),
                expectedValues,
                (record) -> {
                    assertColumn(sink, record, "id0", getTimeType(source, true, 3));
                    assertColumn(sink, record, "id1", getTimeType(source, true, 6));
                    assertColumn(sink, record, "data0", getTimeType(source, false, 3));
                    assertColumn(sink, record, "data1", getTimeType(source, false, 6));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC).toOffsetTime());
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.ORACLE }, reason = "No TIME(n) data type support")
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES }, reason = "Max TIME(n) precision is 6")
    @WithTemporalPrecisionMode
    @Disabled("DBZ-6222 - SQL Server presently does not emit TIME(n) fields with micro or or nano second precision properly")
    public void testNanoTimeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "time(7)",
                List.of("'01:02:03.123456789'", "'14:15:16.456789012'"),
                List.of(OffsetTime.of(1, 2, 3, 123456700, ZoneOffset.UTC), OffsetTime.of(14, 15, 16, 456789000, ZoneOffset.UTC)),
                (record) -> {
                    assertColumn(sink, record, "id", getTimeType(source, true, 7));
                    assertColumn(sink, record, "data", getTimeType(source, false, 7));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC).toOffsetTime());
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.SQLSERVER }, reason = "TIMESTAMP is an internal type and isn't the same as TIMESTAMP(n)")
    @WithTemporalPrecisionMode
    @SuppressWarnings("DataFlowIssue")
    public void testTimestampDataType(Source source, Sink sink) throws Exception {
        final List<String> values;
        if (SourceType.ORACLE.is(source.getType())) {
            values = List.of("TO_TIMESTAMP('2023-03-01 01:02:03.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                    "TO_TIMESTAMP('2022-12-31 14:15:16.456789', 'YYYY-MM-DD HH24:MI:SS.FF6')");
        }
        else {
            values = List.of("'2023-03-01 01:02:03.123456'", "'2022-12-31 14:15:16.456789'");
        }

        final List<OffsetDateTime> expectedValues;
        if (SourceType.MYSQL.is(source.getType())) {
            // ZonedTimestamp does not appear to include any precision beyond seconds for TIMESTAMP
            expectedValues = List.of(OffsetDateTime.of(2023, 3, 1, 1, 2, 3, 0, ZoneOffset.UTC),
                    OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 0, ZoneOffset.UTC));
        }
        else if (source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT) {
            // There is always a loss of precision on time(p) where p > 3 using connect precision
            expectedValues = List.of(OffsetDateTime.of(2023, 3, 1, 1, 2, 3, 123000000, ZoneOffset.UTC),
                    OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456000000, ZoneOffset.UTC));
        }
        else {
            expectedValues = List.of(OffsetDateTime.of(2023, 3, 1, 1, 2, 3, 123456000, ZoneOffset.UTC),
                    OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456789000, ZoneOffset.UTC));
        }

        if (SourceType.MYSQL.is(source.getType()) && SinkType.ORACLE.is(sink.getType())) {
            // MySQL emits "timestamp" as a ZonedTimestamp and this implies a "timestamp with time zone"
            // column; which Oracle does not permit to exist as a primary key. In this use case, only
            // test the data mapping as a non-primary key column.
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "timestamp",
                    List.of(values.get(1)),
                    List.of(expectedValues.get(1)),
                    (record) -> assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6)),
                    (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
        }
        else {
            assertDataType(source,
                    sink,
                    "timestamp",
                    values,
                    expectedValues,
                    (record) -> {
                        if (SourceType.MYSQL.is(source.getType())) {
                            // MySQL emits these as ZonedTimestamp
                            assertColumn(sink, record, "id", getTimestampWithTimezoneType(source, true, 6));
                            assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6));
                        }
                        else {
                            assertColumn(sink, record, "id", getTimestampType(source, true, 6));
                            assertColumn(sink, record, "data", getTimestampType(source, false, 6));
                        }
                    },
                    (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
        }
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.SQLSERVER }, reason = "No TIMESTAMP(n) data type support")
    @WithTemporalPrecisionMode
    @SuppressWarnings("DataFlowIssue")
    public void testTimestampWithPrecisionDataType(Source source, Sink sink) throws Exception {
        String value = "'2022-12-31 14:15:16.456789'";
        if (SourceType.ORACLE.is(source.getType())) {
            value = String.format("TO_TIMESTAMP(%s,'YYYY-MM-DD HH24:MI:SS.FF6')", value);
        }

        // 6 values, for timestamp(1) through timestamp(6)
        final List<String> values = List.of(value, value, value, value, value, value);

        // 6 expected values
        final List<OffsetDateTime> expectedValues = new ArrayList<>();
        expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 500000000, ZoneOffset.UTC)); // timestamp(1)
        expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 460000000, ZoneOffset.UTC)); // timestamp(2)
        expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 457000000, ZoneOffset.UTC)); // timestamp(3)
        if (source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT && !source.getType().is(SourceType.MYSQL)) {
            // Connect precision is lost above 3.
            // MySQL source skipped because TIMESTAMP(4)-TIMESTAMP(6) emitted as ZonedTimestamp (non-Connect type)
            // This is because timestamp in MySQL is timestamp with time zone.
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456000000, ZoneOffset.UTC)); // timestamp(4)
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456000000, ZoneOffset.UTC)); // timestamp(5)
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456000000, ZoneOffset.UTC)); // timestamp(6)
        }
        else {
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456800000, ZoneOffset.UTC)); // timestamp(4)
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456790000, ZoneOffset.UTC)); // timestamp(5)
            expectedValues.add(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456789000, ZoneOffset.UTC)); // timestamp(6)
        }

        if (SourceType.MYSQL.is(source.getType()) && SinkType.ORACLE.is(sink.getType())) {
            // Oracle does not permit using timestamp(n) with time zone as a primary key column, which MySQL
            // emits because it uses ZonedTimestamp for "timestamp" columns. In this case, we will instead
            // only assert values in the non-key fields.
            assertDataTypesNonKeyOnly(source,
                    sink,
                    List.of("timestamp(1)", "timestamp(2)", "timestamp(3)", "timestamp(4)", "timestamp(5)", "timestamp(6)"),
                    values,
                    expectedValues,
                    (record) -> {
                        // MySQL timestamp(n) is timestamp with timezone.
                        // So these get emitted as ZonedTimestamp and need to be handled as such here.
                        assertColumn(sink, record, "data0", getTimestampWithTimezoneType(source, false, 1));
                        assertColumn(sink, record, "data1", getTimestampWithTimezoneType(source, false, 2));
                        assertColumn(sink, record, "data2", getTimestampWithTimezoneType(source, false, 3));
                        assertColumn(sink, record, "data3", getTimestampWithTimezoneType(source, false, 4));
                        assertColumn(sink, record, "data4", getTimestampWithTimezoneType(source, false, 5));
                        assertColumn(sink, record, "data5", getTimestampWithTimezoneType(source, false, 6));
                    },
                    (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
        }
        else {
            assertDataTypes(source,
                    sink,
                    List.of("timestamp(1)", "timestamp(2)", "timestamp(3)", "timestamp(4)", "timestamp(5)", "timestamp(6)"),
                    values,
                    expectedValues,
                    (record) -> {
                        if (SourceType.MYSQL.is(source.getType())) {
                            // MySQL timestamp(n) is timestamp with timezone.
                            // So these get emitted as ZonedTimestamp and need to be handled as such here.
                            assertColumn(sink, record, "id0", getTimestampWithTimezoneType(source, true, 1));
                            assertColumn(sink, record, "id1", getTimestampWithTimezoneType(source, true, 2));
                            assertColumn(sink, record, "id2", getTimestampWithTimezoneType(source, true, 3));
                            assertColumn(sink, record, "id3", getTimestampWithTimezoneType(source, true, 4));
                            assertColumn(sink, record, "id4", getTimestampWithTimezoneType(source, true, 5));
                            assertColumn(sink, record, "id5", getTimestampWithTimezoneType(source, true, 6));
                            assertColumn(sink, record, "data0", getTimestampWithTimezoneType(source, false, 1));
                            assertColumn(sink, record, "data1", getTimestampWithTimezoneType(source, false, 2));
                            assertColumn(sink, record, "data2", getTimestampWithTimezoneType(source, false, 3));
                            assertColumn(sink, record, "data3", getTimestampWithTimezoneType(source, false, 4));
                            assertColumn(sink, record, "data4", getTimestampWithTimezoneType(source, false, 5));
                            assertColumn(sink, record, "data5", getTimestampWithTimezoneType(source, false, 6));
                        }
                        else {
                            assertColumn(sink, record, "id0", getTimestampType(source, true, 1));
                            assertColumn(sink, record, "id1", getTimestampType(source, true, 2));
                            assertColumn(sink, record, "id2", getTimestampType(source, true, 3));
                            assertColumn(sink, record, "id3", getTimestampType(source, true, 4));
                            assertColumn(sink, record, "id4", getTimestampType(source, true, 5));
                            assertColumn(sink, record, "id5", getTimestampType(source, true, 6));
                            assertColumn(sink, record, "data0", getTimestampType(source, false, 1));
                            assertColumn(sink, record, "data1", getTimestampType(source, false, 2));
                            assertColumn(sink, record, "data2", getTimestampType(source, false, 3));
                            assertColumn(sink, record, "data3", getTimestampType(source, false, 4));
                            assertColumn(sink, record, "data4", getTimestampType(source, false, 5));
                            assertColumn(sink, record, "data5", getTimestampType(source, false, 6));
                        }
                    },
                    (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
        }
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No TIMESTAMPTZ data type support")
    @WithTemporalPrecisionMode
    @SuppressWarnings("DataFlowIssue")
    public void testTimestampTzDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        assertDataTypeNonKeyOnly(source,
                sink,
                "timestamptz",
                List.of("'2022-12-31 14:15:16.456789Z'"),
                List.of(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456789000, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.SQLSERVER }, reason = "No TIMESTAMP(n) WITH TIME ZONE data type support")
    @WithTemporalPrecisionMode
    @SuppressWarnings("DataFlowIssue")
    public void testTimestampWithTimeZoneDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        String value = "'2022-12-31 14:15:16.456789 -02:00'";
        if (SourceType.ORACLE.is(source.getType())) {
            value = String.format("TO_TIMESTAMP_TZ(%s,'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')", value);
        }
        assertDataTypeNonKeyOnly(source,
                sink,
                "timestamp(6) with time zone",
                List.of(value),
                List.of(OffsetDateTime.of(2022, 12, 31, 16, 15, 16, 456789000, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No TIMESTAMP(n) WITH LOCAL TIME ZONE data type support")
    @WithTemporalPrecisionMode
    @SuppressWarnings("DataFlowIssue")
    public void testTimestampWithLocalTimeZoneDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "TO_TIMESTAMP('2022-12-31 14:15:16.456789', 'YYYY-MM-DD HH24:MI:SS.FF6')";
        assertDataTypeNonKeyOnly(source,
                sink,
                "timestamp(6) with local time zone",
                List.of(value),
                List.of(OffsetDateTime.of(2022, 12, 31, 14, 15, 16, 456789000, ZoneOffset.UTC).toLocalDateTime()),
                (record) -> assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toLocalDateTime());
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No TIME(n) WITH TIME ZONE data type support")
    @SkipWhenSink(value = { SinkType.MYSQL }, reason = "MySQL has no support for TIME(n) with TIME ZONE support")
    @SkipWhenSink(value = { SinkType.DB2 }, reason = "There is an issue with Daylight Savings Time")
    @WithTemporalPrecisionMode
    public void testTimeWithTimeZoneDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        String value = "'14:15:16.456789 -00:00'";
        if (SourceType.ORACLE.is(source.getType())) {
            value = String.format("TO_TIMESTAMP_TZ(%s,'HH24:MI:SS.FF6 TZH:TZM')", value);
        }

        final int nanoSeconds;
        if (sink.getType().is(SinkType.SQLSERVER)) {
            // SQL Server maps to DATETIME, has a max millisecond precision (.xxx), rounded.
            nanoSeconds = 457000000;
        }
        else if (sink.getType().is(SinkType.DB2)) {
            // DB2 maps to TIME, which only seems to have second precision (.0000000).
            // Additionally DB2 does not support a TIME WITH TIME ZONE data type.
            nanoSeconds = 0;
        }
        else {
            nanoSeconds = 456789000;
        }

        assertDataTypeNonKeyOnly(source,
                sink,
                "time(6) with time zone",
                List.of(value),
                List.of(OffsetTime.of(14, 15, 16, nanoSeconds, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimeWithTimezoneType()),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC).toOffsetTime());
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE }, reason = "No DATETIME data type support")
    @WithTemporalPrecisionMode
    public void testDateTimeDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456'";

        final int precision;
        if (SourceType.MYSQL.is(source.getType()) && source.getOptions().isColumnTypePropagated()) {
            precision = 6;
        }
        else {
            precision = 3;
        }

        // DATETIME emitted as Timestamp, that uses second-based precision
        final int nanosOfSeconds = source.getType().is(SourceType.MYSQL) ? 0 : 457000000;
        assertDataTypeNonKeyOnly(source,
                sink,
                "datetime",
                List.of(value),
                List.of(OffsetDateTime.of(2023, 3, 1, 14, 15, 16, nanosOfSeconds, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampType(source, false, precision)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.POSTGRES, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No DATETIME(n) data type support")
    @WithTemporalPrecisionMode
    public void testDateTimeWithPrecisionDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456789'";

        final List<String> typeNames = List.of("datetime(1)", "datetime(2)", "datetime(3)",
                "datetime(4)", "datetime(5)", "datetime(6)");

        final List<String> values = List.of(value, value, value, value, value, value);

        final boolean connect = TemporalPrecisionMode.CONNECT.equals(source.getOptions().getTemporalPrecisionMode());
        final List<OffsetDateTime> expectedValues = List.of(
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 500000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 460000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 457000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456800000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456790000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456789000, ZoneOffset.UTC));

        assertDataTypesNonKeyOnly(source,
                sink,
                typeNames,
                values,
                expectedValues,
                (record) -> {
                    assertColumn(sink, record, "data0", getTimestampType(source, false, 1));
                    assertColumn(sink, record, "data1", getTimestampType(source, false, 2));
                    assertColumn(sink, record, "data2", getTimestampType(source, false, 3));
                    assertColumn(sink, record, "data3", getTimestampType(source, false, 4));
                    assertColumn(sink, record, "data4", getTimestampType(source, false, 5));
                    assertColumn(sink, record, "data5", getTimestampType(source, false, 6));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No DATETIME2 data type support")
    @WithTemporalPrecisionMode
    public void testDateTime2DataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456789Z'";

        int nanosOfSeconds = 456789000;
        if (source.getOptions().getTemporalPrecisionMode() == TemporalPrecisionMode.CONNECT) {
            nanosOfSeconds = 456000000;
        }

        assertDataTypeNonKeyOnly(source,
                sink,
                "datetime2",
                List.of(value),
                List.of(OffsetDateTime.of(2023, 3, 1, 14, 15, 16, nanosOfSeconds, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No DATETIME2(n) data type support")
    @WithTemporalPrecisionMode
    public void testDateTime2WithPrecisionDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456789123Z'";

        final List<String> typeNames = List.of("datetime2(1)", "datetime2(2)", "datetime2(3)",
                "datetime2(4)", "datetime2(5)", "datetime2(6)",
                "datetime2(7)");

        final List<String> values = List.of(value, value, value, value, value, value, value);

        int dateTime7NanoSeconds = 456789000;
        if (source.getOptions().isColumnTypePropagated() && SinkType.SQLSERVER.is(sink.getType())) {
            dateTime7NanoSeconds = 456789100;
        }

        final boolean connect = TemporalPrecisionMode.CONNECT.equals(source.getOptions().getTemporalPrecisionMode());
        final List<OffsetDateTime> expectedValues = List.of(
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 500000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 460000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 457000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456800000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456790000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : 456789000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, connect ? 456000000 : dateTime7NanoSeconds, ZoneOffset.UTC));

        assertDataTypesNonKeyOnly(source,
                sink,
                typeNames,
                values,
                expectedValues,
                (record) -> {
                    assertColumn(sink, record, "data0", getTimestampType(source, false, 1));
                    assertColumn(sink, record, "data1", getTimestampType(source, false, 2));
                    assertColumn(sink, record, "data2", getTimestampType(source, false, 3));
                    assertColumn(sink, record, "data3", getTimestampType(source, false, 4));
                    assertColumn(sink, record, "data4", getTimestampType(source, false, 5));
                    assertColumn(sink, record, "data5", getTimestampType(source, false, 6));
                    assertColumn(sink, record, "data6", getTimestampType(source, false, 6));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No DATETIMEOFFSET data type support")
    @WithTemporalPrecisionMode
    public void testDateTimeOffsetDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456789Z'";
        assertDataTypeNonKeyOnly(source,
                sink,
                "datetimeoffset",
                List.of(value),
                List.of(OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 456789000, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampWithTimezoneType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No DATETIMEOFFSET(n) data type support")
    @WithTemporalPrecisionMode
    public void testDateTimeOffsetWithPrecisionDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16.456789123Z'";

        final List<String> typeNames = List.of("datetimeoffset(1)", "datetimeoffset(2)", "datetimeoffset(3)",
                "datetimeoffset(4)", "datetimeoffset(5)", "datetimeoffset(6)",
                "datetimeoffset(7)");

        final List<String> values = List.of(value, value, value, value, value, value, value);

        final List<OffsetDateTime> expectedValues = List.of(
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 500000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 460000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 457000000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 456800000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 456790000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 456789000, ZoneOffset.UTC),
                OffsetDateTime.of(2023, 3, 1, 14, 15, 16, 456789000, ZoneOffset.UTC));

        assertDataTypesNonKeyOnly(source,
                sink,
                typeNames,
                values,
                expectedValues,
                (record) -> {
                    assertColumn(sink, record, "data0", getTimestampWithTimezoneType(source, false, 1));
                    assertColumn(sink, record, "data1", getTimestampWithTimezoneType(source, false, 2));
                    assertColumn(sink, record, "data2", getTimestampWithTimezoneType(source, false, 3));
                    assertColumn(sink, record, "data3", getTimestampWithTimezoneType(source, false, 4));
                    assertColumn(sink, record, "data4", getTimestampWithTimezoneType(source, false, 5));
                    assertColumn(sink, record, "data5", getTimestampWithTimezoneType(source, false, 6));
                    assertColumn(sink, record, "data6", getTimestampWithTimezoneType(source, false, 6));
                },
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.ORACLE }, reason = "No SMALLDATETIME data type support")
    @WithTemporalPrecisionMode
    public void testSmallDateTimeDataType(Source source, Sink sink) throws Exception {
        // Only test non-keys because Oracle does not permit timestamp with timezone as primary key columns
        final String value = "'2023-03-01 14:15:16'";
        assertDataTypeNonKeyOnly(source,
                sink,
                "smalldatetime", // minute precision
                List.of(value),
                List.of(OffsetDateTime.of(2023, 3, 1, 14, 15, 0, 0, ZoneOffset.UTC)),
                (record) -> assertColumn(sink, record, "data", getTimestampType(source, false, 6)),
                (rs, index) -> getTimestamp(rs, index).toInstant().atOffset(ZoneOffset.UTC));
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No INTERVAL data type support")
    public void testIntervalDataTypeIntervalHandlingModeNumeric(Source source, Sink sink) throws Exception {
        if (sink.getType().is(SinkType.POSTGRES)) {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval",
                    List.of("'P1Y2M3DT4H5M6.78S'::INTERVAL"),
                    List.of("10303:05:06"),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getString);
        }
        else {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval",
                    List.of("'P1Y2M3DT4H5M6.78S'::INTERVAL"),
                    List.of(MicroDuration.durationMicros(1, 2, 3, 4, 5, 6.78, 365.25 / 12.0)),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getLong);
        }
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No INTERVAL data type support")
    public void testIntervalDataTypeIntervalHandlingModeString(Source source, Sink sink) throws Exception {
        assertDataTypeNonKeyOnly(source,
                sink,
                "interval",
                List.of("'P1Y2M3DT4H5M6.78S'::INTERVAL"),
                List.of("P1Y2M3DT4H5M6.78S"),
                (config) -> config.with("interval.handling.mode", "string"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No INTERVAL DAY(m) TO SECOND data type support")
    public void testIntervalDayToSecondDataTypeIntervalHandlingModeNumeric(Source source, Sink sink) throws Exception {
        // todo: Should we attempt to map this to the proper data type for Oracle sinks?
        if (sink.getType().is(SinkType.POSTGRES)) {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval day to second",
                    List.of("TO_DSINTERVAL('P10DT50H99M1000.365S')"),
                    List.of("291:55:40"),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getString);
        }
        else {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval day to second",
                    List.of("TO_DSINTERVAL('P10DT50H99M1000.365S')"),
                    List.of(1050940365000L),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getLong);
        }
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No INTERVAL DAY(m) TO SECOND data type support")
    public void testIntervalDayToSecondDataTypeIntervalHandlingModeString(Source source, Sink sink) throws Exception {
        // todo: Should we attempt to map this to the proper data type for Oracle sinks?
        assertDataTypeNonKeyOnly(source,
                sink,
                "interval day to second",
                List.of("TO_DSINTERVAL('P10DT50H99M1000.365S')"),
                List.of("P0Y0M12DT3H55M40.365S"),
                (config) -> config.with("interval.handling.mode", "string"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No INTERVAL YEAR(m) TO MONTH data type support")
    public void testIntervalYearToMonthDataTypeIntervalHandlingModeNumeric(Source source, Sink sink) throws Exception {
        // todo: Should we attempt to map this to the proper data type for Oracle sinks?
        if (sink.getType().is(SinkType.POSTGRES)) {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval year to month",
                    List.of("INTERVAL '10-2' YEAR TO MONTH"),
                    List.of("89121:00:00"),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getString);
        }
        else {
            assertDataTypeNonKeyOnly(source,
                    sink,
                    "interval year to month",
                    List.of("INTERVAL '10-2' YEAR TO MONTH"),
                    List.of(320835600000000L),
                    (config) -> config.with("interval.handling.mode", "numeric"),
                    (record) -> assertColumn(sink, record, "data", getIntervalType(source, true)),
                    ResultSet::getLong);
        }
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.POSTGRES, SourceType.SQLSERVER }, reason = "No INTERVAL YEAR(m) TO MONTH data type support")
    public void testIntervalYearToMonthDataTypeIntervalHandlingModeString(Source source, Sink sink) throws Exception {
        // todo: Should we attempt to map this to the proper data type for Oracle sinks?
        assertDataTypeNonKeyOnly(source,
                sink,
                "interval year to month",
                List.of("INTERVAL '10-2' YEAR TO MONTH"),
                List.of("P10Y2M0DT0H0M0S"),
                (config) -> config.with("interval.handling.mode", "string"),
                (record) -> assertColumn(sink, record, "data", getTextType()),
                ResultSet::getString);
    }

    @TestTemplate
    @SkipWhenSource(value = { SourceType.MYSQL, SourceType.ORACLE, SourceType.SQLSERVER }, reason = "No BYTEA data type support")
    @SkipWhenSink(value = { SinkType.MYSQL, SinkType.ORACLE, SinkType.DB2 }, reason = "These data types are not allowed in the primary keys")
    public void testByteaDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "bytea",
                List.of("'hello'"),
                List.of("hello".getBytes(StandardCharsets.UTF_8)),
                (record) -> {
                    assertColumn(sink, record, "id", getBinaryType(source, "bytea"));
                    assertColumn(sink, record, "data", getBinaryType(source, "bytea"));
                },
                ResultSet::getBytes);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The OID data type only applies to PostgreSQL")
    public void testOidDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "oid",
                List.of(3802),
                (record) -> {
                    assertColumn(sink, record, "id", getInt64Type());
                    if (source.getOptions().isColumnTypePropagated() && sink.getType().is(SinkType.POSTGRES)) {
                        assertColumn(sink, record, "data", "OID");
                    }
                    else {
                        assertColumn(sink, record, "data", getInt64Type());
                    }
                },
                ResultSet::getInt);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The LTREE data type only applies to PostgreSQL")
    @WithPostgresExtension("ltree")
    public void testLtreeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "ltree",
                List.of("'abc.xyz'"),
                List.of("abc.xyz"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    if (sink.getType().is(SinkType.POSTGRES)) {
                        assertColumn(sink, record, "id", "LTREE");
                        assertColumn(sink, record, "data", "LTREE");
                    }
                    else {
                        assertColumn(sink, record, "id", getStringType(source, true, false));
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The CITEXT data type only applies to PostgreSQL")
    @WithPostgresExtension("citext")
    public void testCaseInsensitiveDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "citext",
                List.of("'AbCd'"),
                List.of("AbCd"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "CITEXT");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The INET data type only applies to PostgreSQL")
    public void testInetDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "inet",
                List.of("'192.168.1.0'"),
                List.of("192.168.1.0"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "INET");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The INT4RANGE data type only applies to PostgreSQL")
    public void testInt4RangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "int4range",
                List.of("'[1000,6000)'"),
                List.of("[1000,6000)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "INT4RANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The INT8RANGE data type only applies to PostgreSQL")
    public void testInt8RangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "int8range",
                List.of("'[1000000,6000000)'"),
                List.of("[1000000,6000000)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "INT8RANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The NUMRANGE data type only applies to PostgreSQL")
    public void testNumrangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "numrange",
                List.of("'[5.3,6.3)'"),
                List.of("[5.3,6.3)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "NUMRANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The TSRANGE data type only applies to PostgreSQL")
    public void testTsrangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "tsrange",
                List.of("'[2019-03-31 15:30:00,infinity)'"),
                List.of("[\"2019-03-31 15:30:00\",infinity)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "TSRANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The TSTZRANGE data type only applies to PostgreSQL")
    public void testTstzrangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "tstzrange",
                List.of("'[2017-06-05 11:29:12.549426+00,)'"),
                List.of("[\"2017-06-05 11:29:12.549426+00\",)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "TSTZRANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The DATERANGE data type only applies to PostgreSQL")
    public void testDaterangeDataType(Source source, Sink sink) throws Exception {
        assertDataType(source,
                sink,
                "daterange",
                List.of("'[2019-03-31, infinity)'"),
                List.of("[2019-03-31,infinity)"),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    assertColumn(sink, record, "id", getStringType(source, true, false));
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "DATERANGE");
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, false, true));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The HSTORE data type only applies to PostgreSQL")
    @WithPostgresExtension("hstore")
    public void testHstoreDataType(Source source, Sink sink) throws Exception {
        String expectedValue = "{\"key\":\"val\"}";
        if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
            // when sinking to PostgreSQL, it will be returned in HSTORE format rather than JSON
            expectedValue = "\"key\"=>\"val\"";
        }
        else if (sink.getType().is(SinkType.MYSQL)) {
            expectedValue = "{\"key\": \"val\"}";
        }

        assertDataTypeNonKeyOnly(source,
                sink,
                "hstore",
                List.of("'\"key\" => \"val\"'::hstore"),
                List.of(expectedValue),
                (config) -> config.with("include.unknown.datatypes", true),
                (record) -> {
                    // Debezium emits HSTORE data as io.debezium.data.Json logical types.
                    // This is why when column propagation isn't enabled, the field is created as JSON.
                    if (sink.getType().is(SinkType.POSTGRES) && source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", "HSTORE");
                    }
                    else {
                        assertColumn(sink, record, "data", getJsonbType(source));
                    }
                },
                ResultSet::getString);
    }

    @TestTemplate
    @ForSource(value = { SourceType.POSTGRES }, reason = "The HSTORE data type only applies to PostgreSQL")
    @WithPostgresExtension("hstore")
    public void testHstoreWithMapModeDataType(Source source, Sink sink) throws Exception {
        // NOTE:
        // PostgreSQL supports the notion of storing key/value tuples in a data type called HSTORE,
        // and this data type can be emitted as JSON, which can be seen in #testHstoreDataType, but
        // the column can be emitted using map-mode where it uses the Kafka Connect MAP schema type
        // to hold a string-based map of key/value tuples.
        //
        // Not all sink databases support JSON or HSTORE column types, so the following rules have
        // been put in place to support emitting map-mode HSTORE column values across all sinks:
        //
        // 1. Sink is PostgreSQL, create sink column as HSTORE and serialize the map as key/value tuples.
        // 2. Sink is MySQL, cerate sink column as JSON and serialize the map as json data.
        // 3. All other sinks, create sink column as text-based type and serialize the map as json string.
        //
        // see io.debezium.connector.dialect.postgres.MapToHstoreType (option 1)
        // see io.debezium.connector.dialect.mysql.MapToJsonType (option 2)
        // see io.debezium.connector.jdbc.type.connect.ConnectMapToConnectStringType (option 3)
        //
        String expectedValue = "{\"key\":\"val\"}";
        if (sink.getType().is(SinkType.POSTGRES)) {
            // when sinking to PostgreSQL, it will be returned in HSTORE format rather than JSON
            expectedValue = "\"key\"=>\"val\"";
        }
        else if (sink.getType().is(SinkType.MYSQL)) {
            // when sinking to MySQL, it will be returned in JSON format
            expectedValue = "{\"key\": \"val\"}";
        }

        assertDataTypeNonKeyOnly(source,
                sink,
                "hstore",
                List.of("'\"key\" => \"val\"'::hstore"),
                List.of(expectedValue),
                (config) -> {
                    config.with("include.unknown.datatypes", true);
                    config.with("hstore.handling.mode", "map");
                },
                (record) -> {
                    // Debezium emits HSTORE data as MAP schema types.
                    if (sink.getType().is(SinkType.POSTGRES)) {
                        assertColumn(sink, record, "data", "HSTORE");
                    }
                    else if (sink.getType().is(SinkType.MYSQL)) {
                        // MySQL will map the MAP schema types to JSON
                        assertColumn(sink, record, "data", getJsonType(source));
                    }
                    else {
                        // Other sink connectors will serialize the MAP as JSON into TEXT types
                        assertColumn(sink, record, "data", getTextType());
                    }
                },
                ResultSet::getString);
    }

    // todo: remaining data types need tests and/or type system mapping support
    // GEOMETRY (MySql/PostgreSQL)
    // LINESTRING (MySQL)
    // POLYGON (MySQL)
    // MULTIPOINT (MySQL)
    // MULTILINESTRING (MySQL)
    // MULTIPOLYGON (MySQL)
    // GEOMETRYCOLLECTION (MySQL)
    // POINT (PostgreSQL)
    // ROWID (Oracle)

    protected int getMaxDecimalPrecision() {
        return 38;
    }

    protected abstract String getBooleanType();

    protected abstract String getBitsDataType();

    protected abstract String getInt8Type();

    protected abstract String getInt16Type();

    protected abstract String getInt32Type();

    protected abstract String getInt64Type();

    protected abstract String getVariableScaleDecimalType();

    protected abstract String getDecimalType();

    protected abstract String getFloat32Type();

    protected abstract String getFloat64Type();

    protected abstract String getCharType(Source source, boolean key, boolean nationalized);

    protected String getStringType(Source source, boolean key, boolean nationalized) {
        return getStringType(source, key, nationalized, false);
    }

    protected abstract String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength);

    protected abstract String getTextType(boolean nationalized);

    protected String getTextType() {
        return getTextType(false);
    }

    protected abstract String getBinaryType(Source source, String sourceDataType);

    protected abstract String getJsonType(Source source);

    protected String getJsonbType(Source source) {
        return getJsonType(source);
    }

    protected abstract String getXmlType(Source source);

    protected abstract String getUuidType(Source source, boolean key);

    protected abstract String getEnumType(Source source, boolean key);

    protected abstract String getSetType(Source source, boolean key);

    protected abstract String getYearType();

    protected abstract String getDateType();

    protected abstract String getTimeType(Source source, boolean key, int precision);

    protected abstract String getTimeWithTimezoneType();

    protected abstract String getTimestampType(Source source, boolean key, int precision);

    protected abstract String getTimestampWithTimezoneType(Source source, boolean key, int precision);

    protected abstract String getIntervalType(Source source, boolean numeric);

    protected boolean isBitCoercedToBoolean() {
        return false;
    }

    protected Timestamp getTimestamp(ResultSet rs, int index) throws SQLException {
        return rs.getTimestamp(index, Calendar.getInstance(getCurrentSinkTimeZone()));
    }

    protected List<String> bitValues(Source source, String... values) {
        switch (source.getType()) {
            case POSTGRES:
                return Arrays.stream(values)
                        .map(v -> "'" + v + "'::bit" + (v.length() > 1 ? "(" + v.length() + ")" : ""))
                        .collect(Collectors.toList());
            case SQLSERVER:
                if (values.length >= 1) {
                    assertThat(values[0].length()).as("SQL Server bit type only supports 1 or 0.").isEqualTo(1);
                }
                return Arrays.stream(values).collect(Collectors.toList());
            default:
                return Arrays.stream(values)
                        .map(v -> "b'" + v + "'")
                        .collect(Collectors.toList());
        }
    }

    protected String charValue(Source source, Sink sink, int size, boolean key, String value) {
        if (SinkType.MYSQL.equals(sink.getType())) {
            if (SourceType.MYSQL.equals(source.getType())) {
                return value;
            }
            else if (!source.getOptions().isColumnTypePropagated()) {
                return Strings.justifyLeft(value, size, ' ');
            }
            else {
                return key ? Strings.justifyLeft(value, size, ' ') : value;
            }
        }
        else if (SourceType.MYSQL.equals(source.getType())) {
            if (source.getOptions().isColumnTypePropagated()) {
                return key ? value : Strings.justifyLeft(value, size, ' ');
            }
            return value;
        }
        else {
            return Strings.justifyLeft(value, size, ' ');
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected String binaryValue(Source source, String dataType, String value) {
        if (SourceType.SQLSERVER.equals(source.getType())) {
            return String.format("CONVERT(%s, %s)", dataType, value);
        }
        return value;
    }

    @SuppressWarnings("SameParameterValue")
    protected byte[] byteArrayPadded(String value, int padding) {
        final ByteBuffer buffer = ByteBuffer.allocate(padding);
        buffer.put(value.getBytes(StandardCharsets.UTF_8));
        return buffer.array();
    }

    protected String dateValue(Source source, int month, int day, int year) {
        if (SourceType.ORACLE.is(source.getType())) {
            return String.format("TO_DATE('%04d-%02d-%02d', 'YYYY-MM-DD')", year, month, day);
        }
        return String.format("'%04d-%02d-%02d'", year, month, day);
    }

    protected String pointValue(ResultSet rs, int index) throws SQLException {
        String result = rs.getString(index);
        if (!Strings.isNullOrEmpty(result)) {
            if (result.startsWith("(") && result.endsWith(")")) {
                result = result.substring(1, result.length() - 1);
                String[] parts = result.split(",");
                if (parts.length == 2) {
                    result = String.format("(%.6f,%.6f)",
                            Float.parseFloat(parts[0]),
                            Float.parseFloat(parts[1]));
                }
            }
        }
        return result;
    }

    protected void registerSourceConnector(Source source, String tableName) {
        registerSourceConnector(source, null, tableName, null);
    }

    private String getSinkTable(SinkRecord record, Sink sink) {
        final String sinkTableName = tableNamingStrategy.resolveTableName(getCurrentSinkConfig(), record);
        // When quoted identifiers is not enabled, PostgreSQL saves table names as lower-case
        return sink.getType().is(SinkType.POSTGRES) ? sinkTableName.toLowerCase() : sinkTableName;
    }

    protected Properties getDefaultSinkConfig(Sink sink) {
        Properties sinkProperties = new Properties();
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_URL, sink.getJdbcUrl());
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_USER, sink.getUsername());
        sinkProperties.put(JdbcSinkConnectorConfig.CONNECTION_PASSWORD, sink.getPassword());
        sinkProperties.put(JdbcSinkConnectorConfig.DATABASE_TIME_ZONE, TestHelper.getSinkTimeZone());
        return sinkProperties;
    }

    @FunctionalInterface
    protected interface DataTypeColumnAssert {
        void assertColumn(SinkRecord record);
    }

    @FunctionalInterface
    protected interface ColumnReader<T> {
        T read(ResultSet rs, int index) throws Exception;
    }

    @FunctionalInterface
    protected interface ConfigurationAdjuster {
        void adjust(ConnectorConfiguration configuration);
    }

    protected void assertColumn(Sink sink, SinkRecord record, String columnName, String columnType) {
        sink.assertColumn(getSinkTable(record, sink), columnName, columnType);
    }

    protected void assertColumn(Sink sink, SinkRecord record, String columnName, String columnType, int length) {
        sink.assertColumn(getSinkTable(record, sink), columnName, columnType, length);
    }

    protected void assertColumn(Sink sink, SinkRecord record, String columnName, String columnType, int precision, int scale) {
        sink.assertColumn(getSinkTable(record, sink), columnName, columnType, precision, scale);
    }

    protected <T> void assertDataType(Source source, Sink sink, String typeName, List<T> values, DataTypeColumnAssert columnAssert,
                                      ColumnReader<T> columnReader)
            throws Exception {
        assertDataType(source, sink, typeName, values, values, null, columnAssert, columnReader);
    }

    protected <T> void assertDataTypes(Source source, Sink sink, List<String> typeNames, List<T> values, DataTypeColumnAssert columnAssert,
                                       ColumnReader<T> columnReader)
            throws Exception {
        assertDataTypes(source, sink, typeNames, values, values, null, columnAssert, columnReader);
    }

    protected <T, U> void assertDataType(Source source, Sink sink, String typeName, List<T> values, List<U> expectedValues,
                                         DataTypeColumnAssert columnAssert, ColumnReader<U> columnReader)
            throws Exception {
        assertDataType(source, sink, typeName, values, expectedValues, null, columnAssert, columnReader);
    }

    protected <T, U> void assertDataTypes(Source source, Sink sink, List<String> typeNames, List<T> values, List<U> expectedValues,
                                          DataTypeColumnAssert columnAssert, ColumnReader<U> columnReader)
            throws Exception {
        assertDataTypes(source, sink, typeNames, values, expectedValues, null, columnAssert, columnReader);
    }

    protected <T, U> void assertDataTypeNonKeyOnly(Source source, Sink sink, String typeName, List<T> values, List<U> expectedValues,
                                                   DataTypeColumnAssert columnAssert, ColumnReader<U> columnReader)
            throws Exception {
        assertDataTypeNonKeyOnly(source, sink, typeName, values, expectedValues, null, columnAssert, columnReader);
    }

    protected <T, U> void assertDataTypesNonKeyOnly(Source source, Sink sink, List<String> typeNames, List<T> values, List<U> expectedValues,
                                                    DataTypeColumnAssert columnAssert, ColumnReader<U> columnReader)
            throws Exception {
        assertDataTypesNonKeyOnly(source, sink, typeNames, values, expectedValues, null, columnAssert, columnReader);
    }

    @SuppressWarnings("SameParameterValue")
    protected <T> void assertDataType(Source source, Sink sink, String typeName, List<T> values, ConfigurationAdjuster configAdjuster,
                                      DataTypeColumnAssert columnAssert, ColumnReader<T> columnReader)
            throws Exception {
        assertDataType(source, sink, typeName, values, values, configAdjuster, columnAssert, columnReader);
    }

    protected boolean skipDefaultValues(String typeName) {
        return Arrays.asList("smallserial", "serial", "bigserial",
                "json",
                "tinytext", "mediumtext", "longtext",
                "text",
                "tinyblob", "mediumblob", "longblob",
                // todo: apply a default value for this seems to cause Oracle to fail to emit events
                "interval year to month").contains(typeName);
    }

    protected <T, U> void assertDataType(Source source, Sink sink, String typeName, List<T> values, List<U> expectedValues,
                                         ConfigurationAdjuster configAdjuster, DataTypeColumnAssert columnAssert,
                                         ColumnReader<U> columnReader)
            throws Exception {
        final String tableName = source.randomTableName();

        final String createSql;
        if (!source.getOptions().useDefaultValues() || skipDefaultValues(typeName)) {
            createSql = String.format("CREATE TABLE %s (id %s, data %s, primary key(id))", tableName, typeName, typeName);
        }
        else {
            createSql = String.format("CREATE TABLE %s (id %s, data %s DEFAULT %s NOT NULL, primary key(id))", tableName, typeName, typeName, values.get(0));
        }

        final String insertSql = String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", values));

        registerSourceConnector(source, Collections.singletonList(typeName), tableName, configAdjuster, createSql, insertSql);

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSink(source, sinkProperties, tableName);

        consumeAndAssert(sink, columnAssert, expectedValues, columnReader);
    }

    @SuppressWarnings("SameParameterValue")
    protected <T, U> void assertDataTypeNonKeyOnly(Source source, Sink sink, String typeName, List<T> values, List<U> expectedValues,
                                                   ConfigurationAdjuster configAdjuster, DataTypeColumnAssert columnAssert,
                                                   ColumnReader<U> columnReader)
            throws Exception {
        final String tableName = source.randomTableName();

        final String createSql;
        final String insertSql;
        if (isLobTypeName(typeName)) {
            // Oracle LOB columns require a primary key to be streamed.
            createSql = String.format("CREATE TABLE %s (data %s, id integer, primary key(id))", tableName, typeName);
            insertSql = String.format("INSERT INTO %s VALUES (%s, 1)", tableName, Strings.join(",", values));
        }
        else {
            if (!source.getOptions().useDefaultValues() || skipDefaultValues(typeName)) {
                createSql = String.format("CREATE TABLE %s (data %s NOT NULL)", tableName, typeName);
            }
            else {
                createSql = String.format("CREATE TABLE %s (data %s DEFAULT %s NOT NULL)", tableName, typeName, values.get(0));
            }
            insertSql = String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", values));
        }

        registerSourceConnector(source, Collections.singletonList(typeName), tableName, configAdjuster, createSql, insertSql);

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSink(source, sinkProperties, tableName);

        consumeAndAssert(sink, columnAssert, expectedValues, columnReader);
    }

    @SuppressWarnings("SameParameterValue")
    protected <T, U> void assertDataTypesNonKeyOnly(Source source, Sink sink, List<String> typeNames, List<T> values, List<U> expectedValues,
                                                    ConfigurationAdjuster configAdjuster, DataTypeColumnAssert columnAssert,
                                                    ColumnReader<U> columnReader)
            throws Exception {
        final String tableName = source.randomTableName();

        final String createSql = createTableFromTypes(source, tableName, false, typeNames, values);
        final String insertSql = String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", values));

        registerSourceConnector(source, typeNames, tableName, configAdjuster, createSql, insertSql);

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.NONE.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.INSERT.getValue());
        startSink(source, sinkProperties, tableName);

        consumeAndAssert(sink, columnAssert, expectedValues, columnReader);
    }

    @SuppressWarnings("SameParameterValue")
    protected <T, U> void assertDataTypes(Source source, Sink sink, List<String> typeNames, List<T> values, List<U> expectedValues,
                                          ConfigurationAdjuster configAdjuster, DataTypeColumnAssert columnAssert,
                                          ColumnReader<U> columnReader)
            throws Exception {
        final String tableName = source.randomTableName();

        final List<T> totalValues = new ArrayList<>();
        for (int i = 0; i < 2; ++i) {
            totalValues.addAll(values);
        }

        final String createSql = createTableFromTypes(source, tableName, true, typeNames, values);
        final String insertSql = String.format("INSERT INTO %s VALUES (%s)", tableName, Strings.join(",", totalValues));

        registerSourceConnector(source, typeNames, tableName, configAdjuster, createSql, insertSql);

        Properties sinkProperties = getDefaultSinkConfig(sink);
        sinkProperties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, SchemaEvolutionMode.BASIC.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, PrimaryKeyMode.RECORD_KEY.getValue());
        sinkProperties.put(JdbcSinkConnectorConfig.INSERT_MODE, InsertMode.UPSERT.getValue());
        startSink(source, sinkProperties, tableName);

        final List<U> totalExpectedValues = new ArrayList<>();
        for (int i = 0; i < 2; ++i) {
            totalExpectedValues.addAll(expectedValues);
        }

        consumeAndAssert(sink, columnAssert, totalExpectedValues, columnReader);
    }

    protected boolean isLobTypeName(String typeName) {
        return typeName.equalsIgnoreCase("CLOB") || typeName.equalsIgnoreCase("NCLOB") || typeName.equalsIgnoreCase("BLOB");
    }

    protected String createTableFromTypes(Source source, String tableName, boolean keys, List<String> typeNames, List<?> values) {
        final StringBuilder create = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");

        if (keys) {
            for (int i = 0; i < typeNames.size(); ++i) {
                create.append("id").append(i).append(" ").append(typeNames.get(i)).append(", ");
            }
        }

        for (int i = 0; i < typeNames.size(); ++i) {
            create.append("data").append(i).append(" ").append(typeNames.get(i));
            if ((i + 1) < typeNames.size()) {
                if (source.getOptions().useDefaultValues()) {
                    create.append(" DEFAULT ").append(values.get(i)).append(" NOT NULL");
                }
                create.append(", ");
            }
        }

        if (keys) {
            create.append(", primary key (");
            for (int i = 0; i < typeNames.size(); ++i) {
                create.append("id").append(i);
                if (i + 1 < typeNames.size()) {
                    create.append(", ");
                }
            }
            create.append(")");
        }

        create.append(")");
        return create.toString();
    }

    protected void registerSourceConnector(Source source, List<String> typeNames, String tableName,
                                           ConfigurationAdjuster configAdjuster, String createSql,
                                           String insertSql)
            throws Exception {
        if (source.getOptions().useSnapshot()) {
            source.execute(createSql);
            source.streamTable(tableName);
            source.execute(insertSql);
            registerSourceConnector(source, typeNames, tableName, configAdjuster);
        }
        else {
            registerSourceConnector(source, typeNames, tableName, configAdjuster);
            source.execute(createSql);
            source.streamTable(tableName);
            source.execute(insertSql);
        }
    }

    protected void registerSourceConnector(Source source, List<String> typeName, String tableName,
                                           ConfigurationAdjuster configAdjuster) {
        // Create default source connector configuration
        final ConnectorConfiguration sourceConfig = getSourceConnectorConfig(source, tableName);
        sourceConfig.with("decimal.handling.mode", DecimalHandlingMode.PRECISE.getValue());
        sourceConfig.with("binary.handling.mode", BinaryHandlingMode.BYTES.getValue());

        // Adjust it if necessary.
        if (configAdjuster != null) {
            configAdjuster.adjust(sourceConfig);
        }

        if (SourceType.ORACLE == source.getType()) {
            // Oracle only emits boolean types when the NumberOneToBooleanConverter is applied.
            sourceConfig.with("converters", "boolean")
                    .with("boolean.type", "io.debezium.connector.oracle.converters.NumberOneToBooleanConverter")
                    .with("boolean.selector", ".*");

            if (typeName != null &&
                    typeName.stream().anyMatch(p -> p.equalsIgnoreCase("CLOB")
                            || p.equalsIgnoreCase("NCLOB")
                            || p.equalsIgnoreCase("BLOB"))) {
                sourceConfig.with("lob.enabled", "true");
            }
        }

        source.registerSourceConnector(sourceConfig);
    }

    protected void applyJdbcSourceConverter(Source source, ConnectorConfiguration config, String booleanSelector, String realSelector, String stringSelector) {
        if (source.getType().is(SourceType.MYSQL)) {
            config.with("converters", "jdbc-sink");
            config.with("jdbc-sink.type", "io.debezium.connector.mysql.converters.JdbcSinkDataTypesConverter");
            if (!Strings.isNullOrEmpty(booleanSelector)) {
                config.with("jdbc-sink.selector.boolean", booleanSelector);
            }
            if (!Strings.isNullOrEmpty(realSelector)) {
                config.with("jdbc-sink.selector.real", realSelector);
            }
            if (!Strings.isNullOrEmpty(stringSelector)) {
                config.with("jdbc-sink.selector.string", stringSelector);
            }
        }
    }

    protected <U> void consumeAndAssert(Sink sink, DataTypeColumnAssert columnAssert, List<U> expectedValues, ColumnReader<U> columnReader) throws Exception {
        final SinkRecord record = consumeSinkRecord();
        columnAssert.assertColumn(record);

        sink.assertRows(getSinkTable(record, sink), rs -> {
            for (int i = 0; i < expectedValues.size(); ++i) {
                final String description = String.format("Column %s read failed.", rs.getMetaData().getColumnName(i + 1));
                assertThat(columnReader.read(rs, i + 1)).as(description).isEqualTo(expectedValues.get(i));
            }
            return null;
        });
    }

    protected void assertCharDataType(Source source, Sink sink, String dataType, boolean nationalized) throws Exception {
        assertDataType(source,
                sink,
                getDataTypeWithCollation(source, dataType, nationalized),
                List.of("'a'", "'b'"),
                List.of("a", "b"),
                (config) -> applyJdbcSourceConverter(source, config, null, null, ".*.id|.*.data"),
                (record) -> {
                    // id uses text type because column propagation doesn't apply for primary keys
                    // this means that the sink connector only sees the type as "STRING" only.
                    assertColumn(sink, record, "id", getCharType(source, true, nationalized));
                    assertColumn(sink, record, "data", getCharType(source, false, nationalized));
                },
                ResultSet::getString);
    }

    @SuppressWarnings("SameParameterValue")
    protected void assertCharWithLengthDataType(Source source, Sink sink, String dataType, int length, boolean nationalized) throws Exception {
        assertDataType(source,
                sink,
                getDataTypeWithCollation(source, dataType, nationalized),
                List.of("'a'", "'b'"),
                List.of(charValue(source, sink, length, true, "a"), charValue(source, sink, length, false, "b")),
                (config) -> applyJdbcSourceConverter(source, config, null, null, ".*.id|.*.data"),
                (record) -> {
                    // id uses text type because column propagation doesn't apply for primary keys
                    // this means that the sink connector only sees the type as "STRING" only.
                    assertColumn(sink, record, "id", getCharType(source, true, nationalized));
                    if (source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", getCharType(source, false, nationalized), length);
                    }
                    else {
                        assertColumn(sink, record, "data", getCharType(source, false, nationalized));
                    }
                },
                ResultSet::getString);
    }

    @SuppressWarnings("SameParameterValue")
    protected void assertVarcharDataType(Source source, Sink sink, String dataType, int length, boolean nationalized) throws Exception {
        assertDataType(source,
                sink,
                getDataTypeWithCollation(source, dataType, nationalized),
                List.of("'abc'", "'hello world'"),
                List.of("abc", "hello world"),
                (config) -> applyJdbcSourceConverter(source, config, null, null, ".*.id|.*.data"),
                (record) -> {
                    // id uses text type because column propagation doesn't apply for primary keys
                    // this means that the sink connector only sees the type as "STRING" only.
                    assertColumn(sink, record, "id", getStringType(source, true, nationalized));
                    if (source.getOptions().isColumnTypePropagated()) {
                        assertColumn(sink, record, "data", getStringType(source, false, nationalized), length);
                    }
                    else {
                        assertColumn(sink, record, "data", getStringType(source, false, nationalized));
                    }
                },
                ResultSet::getString);
    }

    protected String getDataTypeWithCollation(Source source, String dataType, boolean nationalized) {
        // When not explicitly setting a COLLATION, MySQL may default to utf8mbX character sets, and
        // we want to explicitly set the collation based on the nationalized setting to test that
        // specific unique tuple of source column definitions.
        if (source.getType().is(SourceType.MYSQL) && !nationalized) {
            return String.format("%s collate latin1_general_cs", dataType);
        }
        else if (source.getType().is(SourceType.MYSQL)) {
            return String.format("%s collate utf8mb3_general_ci", dataType);
        }
        return dataType;
    }
}

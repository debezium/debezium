/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static io.debezium.data.VariableScaleDecimal.fromLogical;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestWhenRunWithApicurioRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenRunWithApicurio;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.SkipLongRunning;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.relational.ddl.DdlParserListener.TableCreatedEvent;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Collect;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Only applies to LogMiner")
public class HybridMiningStrategyIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipApicurioRule = new SkipTestWhenRunWithApicurioRule();

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    @Rule
    public final TestRule skipLongRunning = new SkipTestRule();

    private OracleConnection connection;
    private DecimalHandlingMode decimalHandlingMode;
    private TemporalPrecisionMode temporalPrecisionMode;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        decimalHandlingMode = DecimalHandlingMode.PRECISE; // default
        temporalPrecisionMode = TemporalPrecisionMode.ADAPTIVE; // default

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();
    }

    @After
    public void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropAllTables();
            connection.close();
        }
    }

    // todo:
    // add clob, blob, and xml support

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesCharacterDataTypes() throws Exception {
        streamOfflineSchemaChanges("varchar(50)",
                QueryValue.ofBind("ABC"), QueryValue.ofBind("XYZ"),
                "ABC", "XYZ");
        streamOfflineSchemaChanges("varchar2(50)",
                QueryValue.ofBind("ABC"), QueryValue.ofBind("XYZ"),
                "ABC", "XYZ");
        streamOfflineSchemaChanges("nvarchar2(50)",
                QueryValue.ofBind("AêñüC"), QueryValue.ofBind("XYZ"),
                "AêñüC", "XYZ");
        streamOfflineSchemaChanges("char(3)",
                QueryValue.ofBind("NO"), QueryValue.ofBind("YES"),
                "NO ", "YES");
        streamOfflineSchemaChanges("nchar(3)",
                QueryValue.ofBind("NO"), QueryValue.ofBind("YES"),
                "NO ", "YES");
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeCharacterDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("varchar(50)",
                QueryValue.ofBind("ABC"), QueryValue.ofBind("XYZ"),
                "ABC", "XYZ");
        streamSchemaChangeMixedWithDataChange("varchar2(50)",
                QueryValue.ofBind("ABC"), QueryValue.ofBind("XYZ"),
                "ABC", "XYZ");
        streamSchemaChangeMixedWithDataChange("nvarchar2(50)",
                QueryValue.ofBind("AêñüC"), QueryValue.ofBind("XYZ"),
                "AêñüC", "XYZ");
        streamSchemaChangeMixedWithDataChange("char(3)",
                QueryValue.ofBind("NO"), QueryValue.ofBind("YES"),
                "NO ", "YES");
        streamSchemaChangeMixedWithDataChange("nchar(3)",
                QueryValue.ofBind("NO"), QueryValue.ofBind("YES"),
                "NO ", "YES");
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamOfflineSchemaChangesFloatingPointDataTypes() throws Exception {
        streamOfflineSchemaChanges("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamOfflineSchemaChanges("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamOfflineSchemaChanges("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                varScaleDecimal("3.33"), varScaleDecimal("4.33"));
        streamOfflineSchemaChanges("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                varScaleDecimal("8.888"), varScaleDecimal("9.999"));
        streamOfflineSchemaChanges("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                new BigDecimal("4.444400"), new BigDecimal("5.555500"));
        streamOfflineSchemaChanges("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                varScaleDecimal("5.555"), varScaleDecimal("6.666"));
        streamOfflineSchemaChanges("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                varScaleDecimal("6.66"), varScaleDecimal("7.77"));
        streamOfflineSchemaChanges("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamOfflineSchemaChanges("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamOfflineSchemaChanges("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                varScaleDecimal("77.323"), varScaleDecimal("88.434"));
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamOfflineSchemaChangesFloatingPointDataTypesAsString() throws Exception {
        // Override DecimalHandlingMode default
        decimalHandlingMode = DecimalHandlingMode.STRING;

        streamOfflineSchemaChanges("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamOfflineSchemaChanges("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamOfflineSchemaChanges("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                "3.33", "4.33");
        streamOfflineSchemaChanges("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                "8.888", "9.999");
        streamOfflineSchemaChanges("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                "4.444400", "5.555500");
        streamOfflineSchemaChanges("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                "5.555", "6.666");
        streamOfflineSchemaChanges("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                "6.66", "7.77");
        streamOfflineSchemaChanges("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                "1234.567891", "2345.678912");
        streamOfflineSchemaChanges("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                "1234.567891", "2345.678912");
        streamOfflineSchemaChanges("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                "77.323", "88.434");
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamOfflineSchemaChangesFloatingPointDataTypesAsDouble() throws Exception {
        // Override DecimalHandlingMode default
        decimalHandlingMode = DecimalHandlingMode.DOUBLE;

        streamOfflineSchemaChanges("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamOfflineSchemaChanges("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamOfflineSchemaChanges("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                3.33d, 4.33d);
        streamOfflineSchemaChanges("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                8.888d, 9.999d);
        streamOfflineSchemaChanges("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                4.4444, 5.5555);
        streamOfflineSchemaChanges("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                5.555, 6.666);
        streamOfflineSchemaChanges("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                6.66, 7.77);
        streamOfflineSchemaChanges("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                1234.567891, 2345.678912);
        streamOfflineSchemaChanges("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                1234.567891, 2345.678912);
        streamOfflineSchemaChanges("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                77.323, 88.434);
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamSchemaChangeWithDataChangeFloatingPointDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamSchemaChangeMixedWithDataChange("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamSchemaChangeMixedWithDataChange("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                varScaleDecimal("3.33"), varScaleDecimal("4.33"));
        streamSchemaChangeMixedWithDataChange("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                varScaleDecimal("8.888"), varScaleDecimal("9.999"));
        streamSchemaChangeMixedWithDataChange("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                new BigDecimal("4.444400"), new BigDecimal("5.555500"));
        streamSchemaChangeMixedWithDataChange("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                varScaleDecimal("5.555"), varScaleDecimal("6.666"));
        streamSchemaChangeMixedWithDataChange("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                varScaleDecimal("6.66"), varScaleDecimal("7.77"));
        streamSchemaChangeMixedWithDataChange("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamSchemaChangeMixedWithDataChange("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                new BigDecimal("1234.567891"), new BigDecimal("2345.678912"));
        streamSchemaChangeMixedWithDataChange("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                varScaleDecimal("77.323"), varScaleDecimal("88.434"));
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamSchemaChangeWithDataChangeFloatingPointDataTypesAsString() throws Exception {
        // Override DecimalHandlingMode default
        decimalHandlingMode = DecimalHandlingMode.STRING;

        streamSchemaChangeMixedWithDataChange("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamSchemaChangeMixedWithDataChange("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamSchemaChangeMixedWithDataChange("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                "3.33", "4.33");
        streamSchemaChangeMixedWithDataChange("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                "8.888", "9.999");
        streamSchemaChangeMixedWithDataChange("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                "4.444400", "5.555500");
        streamSchemaChangeMixedWithDataChange("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                "5.555", "6.666");
        streamSchemaChangeMixedWithDataChange("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                "6.66", "7.77");
        streamSchemaChangeMixedWithDataChange("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                "1234.567891", "2345.678912");
        streamSchemaChangeMixedWithDataChange("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                "1234.567891", "2345.678912");
        streamSchemaChangeMixedWithDataChange("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                "77.323", "88.434");
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamSchemaChangeWithDataChangeFloatingPointDataTypesAsDouble() throws Exception {
        // Override DecimalHandlingMode default
        decimalHandlingMode = DecimalHandlingMode.DOUBLE;

        streamSchemaChangeMixedWithDataChange("binary_float",
                QueryValue.ofBind(3.14f), QueryValue.ofBind(4.14f),
                3.14f, 4.14f);
        streamSchemaChangeMixedWithDataChange("binary_double",
                QueryValue.ofBind(3.14), QueryValue.ofBind(4.14),
                3.14, 4.14);
        streamSchemaChangeMixedWithDataChange("float",
                QueryValue.ofBind(3.33), QueryValue.ofBind(4.33),
                3.33, 4.33);
        streamSchemaChangeMixedWithDataChange("float(10)",
                QueryValue.ofBind(8.888), QueryValue.ofBind(9.999),
                8.888, 9.999);
        streamSchemaChangeMixedWithDataChange("number(10,6)",
                QueryValue.ofBind(4.4444), QueryValue.ofBind(5.5555),
                4.4444, 5.5555);
        streamSchemaChangeMixedWithDataChange("double precision",
                QueryValue.ofBind(5.555), QueryValue.ofBind(6.666),
                5.555, 6.666);
        streamSchemaChangeMixedWithDataChange("real",
                QueryValue.ofBind(6.66), QueryValue.ofBind(7.77),
                6.66, 7.77);
        streamSchemaChangeMixedWithDataChange("decimal(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                1234.567891, 2345.678912);
        streamSchemaChangeMixedWithDataChange("numeric(10,6)",
                QueryValue.ofBind(1234.567891), QueryValue.ofBind(2345.678912),
                1234.567891, 2345.678912);
        streamSchemaChangeMixedWithDataChange("number",
                QueryValue.ofBind(77.323), QueryValue.ofBind(88.434),
                77.323, 88.434);
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipWhenRunWithApicurio
    @SkipLongRunning
    public void shouldStreamOfflineSchemaChangesIntegerDataTypes() throws Exception {
        streamOfflineSchemaChanges("int",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                new BigDecimal("1"), new BigDecimal("2"));
        streamOfflineSchemaChanges("integer",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                new BigDecimal("1"), new BigDecimal("2"));
        streamOfflineSchemaChanges("smallint",
                QueryValue.ofBind(33), QueryValue.ofBind(44),
                new BigDecimal("33"), new BigDecimal("44"));
        streamOfflineSchemaChanges("number(38)",
                QueryValue.ofBind(4444), QueryValue.ofBind(5555),
                new BigDecimal("4444"), new BigDecimal("5555"));
        streamOfflineSchemaChanges("number(38,0)",
                QueryValue.ofBind(4444), QueryValue.ofBind(5555),
                new BigDecimal("4444"), new BigDecimal("5555"));
        streamOfflineSchemaChanges("number(2)",
                QueryValue.ofBind(88), QueryValue.ofBind(99),
                (byte) 88, (byte) 99);
        streamOfflineSchemaChanges("number(4)",
                QueryValue.ofBind(8888), QueryValue.ofBind(9999),
                (short) 8888, (short) 9999);
        streamOfflineSchemaChanges("number(9)",
                QueryValue.ofBind(888888888), QueryValue.ofBind(999999999),
                888888888, 999999999);
        streamOfflineSchemaChanges("number(18)",
                QueryValue.ofBind(888888888888888888L), QueryValue.ofBind(999999999999999999L),
                888888888888888888L, 999999999999999999L);
        streamOfflineSchemaChanges("number(1,-1)",
                QueryValue.ofBind(93), QueryValue.ofBind(94),
                (byte) 90, (byte) 90);
        streamOfflineSchemaChanges("number(2,-2)",
                QueryValue.ofBind(9349), QueryValue.ofBind(9449),
                (short) 9300, (short) 9400);
        streamOfflineSchemaChanges("number(8,-1)",
                QueryValue.ofBind(989999994), QueryValue.ofBind(999999994),
                989999990, 999999990);
        streamOfflineSchemaChanges("number(16,-2)",
                QueryValue.ofBind(989999999999999949L), QueryValue.ofBind(999999999999999949L),
                989999999999999900L, 999999999999999900L);
        streamOfflineSchemaChanges("number(36,-2)",
                QueryValue.ofBind(new BigDecimal(new BigInteger("999999999999999999999999999999999999"), -2)),
                QueryValue.ofBind(new BigDecimal(new BigInteger("999999999999999999999999999999999949"), -2)),
                new BigDecimal(new BigInteger("999999999999999999999999999999999999"), -2),
                new BigDecimal(new BigInteger("999999999999999999999999999999999949"), -2));
        streamOfflineSchemaChanges("decimal(10)",
                QueryValue.ofBind(9899999999L), QueryValue.ofBind(9999999999L),
                9899999999L, 9999999999L);
        streamOfflineSchemaChanges("numeric(10)",
                QueryValue.ofBind(9899999999L), QueryValue.ofBind(9999999999L),
                9899999999L, 9999999999L);
        streamOfflineSchemaChanges("number(1)",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                (byte) 1, (byte) 2);
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipWhenRunWithApicurio
    @SkipLongRunning
    public void shouldStreamSchemaChangeWithDataChangeIntegerDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("int",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                new BigDecimal("1"), new BigDecimal("2"));
        streamSchemaChangeMixedWithDataChange("integer",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                new BigDecimal("1"), new BigDecimal("2"));
        streamSchemaChangeMixedWithDataChange("smallint",
                QueryValue.ofBind(33), QueryValue.ofBind(44),
                new BigDecimal("33"), new BigDecimal("44"));
        streamSchemaChangeMixedWithDataChange("number(38)",
                QueryValue.ofBind(4444), QueryValue.ofBind(5555),
                new BigDecimal("4444"), new BigDecimal("5555"));
        streamSchemaChangeMixedWithDataChange("number(38,0)",
                QueryValue.ofBind(4444), QueryValue.ofBind(5555),
                new BigDecimal("4444"), new BigDecimal("5555"));
        streamSchemaChangeMixedWithDataChange("number(2)",
                QueryValue.ofBind(88), QueryValue.ofBind(99),
                (byte) 88, (byte) 99);
        streamSchemaChangeMixedWithDataChange("number(4)",
                QueryValue.ofBind(8888), QueryValue.ofBind(9999),
                (short) 8888, (short) 9999);
        streamSchemaChangeMixedWithDataChange("number(9)",
                QueryValue.ofBind(888888888), QueryValue.ofBind(999999999),
                888888888, 999999999);
        streamSchemaChangeMixedWithDataChange("number(18)",
                QueryValue.ofBind(888888888888888888L), QueryValue.ofBind(999999999999999999L),
                888888888888888888L, 999999999999999999L);
        streamSchemaChangeMixedWithDataChange("number(1,-1)",
                QueryValue.ofBind(93), QueryValue.ofBind(94),
                (byte) 90, (byte) 90);
        streamSchemaChangeMixedWithDataChange("number(2,-2)",
                QueryValue.ofBind(9349), QueryValue.ofBind(9449),
                (short) 9300, (short) 9400);
        streamSchemaChangeMixedWithDataChange("number(8,-1)",
                QueryValue.ofBind(989999994), QueryValue.ofBind(999999994),
                989999990, 999999990);
        streamSchemaChangeMixedWithDataChange("number(16,-2)",
                QueryValue.ofBind(989999999999999949L), QueryValue.ofBind(999999999999999949L),
                989999999999999900L, 999999999999999900L);
        streamSchemaChangeMixedWithDataChange("number(36,-2)",
                QueryValue.ofBind(new BigDecimal(new BigInteger("999999999999999999999999999999999999"), -2)),
                QueryValue.ofBind(new BigDecimal(new BigInteger("999999999999999999999999999999999949"), -2)),
                new BigDecimal(new BigInteger("999999999999999999999999999999999999"), -2),
                new BigDecimal(new BigInteger("999999999999999999999999999999999949"), -2));
        streamSchemaChangeMixedWithDataChange("decimal(10)",
                QueryValue.ofBind(9899999999L), QueryValue.ofBind(9999999999L),
                9899999999L, 9999999999L);
        streamSchemaChangeMixedWithDataChange("numeric(10)",
                QueryValue.ofBind(9899999999L), QueryValue.ofBind(9999999999L),
                9899999999L, 9999999999L);
        streamSchemaChangeMixedWithDataChange("number(1)",
                QueryValue.ofBind(1), QueryValue.ofBind(2),
                (byte) 1, (byte) 2);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangesTemporalDataTypes() throws Exception {
        streamOfflineSchemaChanges("date",
                QueryValue.ofSql("TO_DATE('2018-03-27','yyyy-mm-dd')"),
                QueryValue.ofSql("TO_DATE('2018-10-15','yyyy-mm-dd')"),
                1_522_108_800_000L,
                1_539_561_600_000L);
        streamOfflineSchemaChanges("timestamp",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890);
        streamOfflineSchemaChanges("timestamp(2)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130);
        streamOfflineSchemaChanges("timestamp(4)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500);
        streamOfflineSchemaChanges("timestamp(9)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789);
        streamOfflineSchemaChanges("timestamp with time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00")),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamOfflineSchemaChanges("timestamp with local time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00")),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamOfflineSchemaChanges("interval year to month",
                QueryValue.ofSql("INTERVAL '-3-6' YEAR TO MONTH"),
                QueryValue.ofSql("INTERVAL '-2-5' YEAR TO MONTH"),
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamOfflineSchemaChanges("interval day(3) to second(2)",
                QueryValue.ofSql("INTERVAL '-1 2:3:4.56' DAY TO SECOND"),
                QueryValue.ofSql("INTERVAL '-2 4:5:6.21' DAY TO SECOND"),
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamOfflineSchemaChangesTemporalDataTypesAsConnect() throws Exception {
        // Override TemporalPrecisionMode default
        temporalPrecisionMode = TemporalPrecisionMode.CONNECT;

        streamOfflineSchemaChanges("date",
                QueryValue.ofSql("TO_DATE('2018-03-27','yyyy-mm-dd')"),
                QueryValue.ofSql("TO_DATE('2018-10-15','yyyy-mm-dd')"),
                Date.from(LocalDate.of(2018, 3, 27).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDate.of(2018, 10, 15).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant()));
        streamOfflineSchemaChanges("timestamp",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 7890 * 1_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 7890 * 1_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamOfflineSchemaChanges("timestamp(2)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 130 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 130 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamOfflineSchemaChanges("timestamp(4)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 125_500 * 1_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 125_500 * 1_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamOfflineSchemaChanges("timestamp(9)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 123456789).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 123456789).atOffset(ZoneOffset.UTC).toInstant()));
        streamOfflineSchemaChanges("timestamp with time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00")),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamOfflineSchemaChanges("timestamp with local time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00")),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamOfflineSchemaChanges("interval year to month",
                QueryValue.ofSql("INTERVAL '-3-6' YEAR TO MONTH"),
                QueryValue.ofSql("INTERVAL '-2-5' YEAR TO MONTH"),
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamOfflineSchemaChanges("interval day(3) to second(2)",
                QueryValue.ofSql("INTERVAL '-1 2:3:4.56' DAY TO SECOND"),
                QueryValue.ofSql("INTERVAL '-2 4:5:6.21' DAY TO SECOND"),
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithDataChangeTemporalDataTypes() throws Exception {
        streamSchemaChangeMixedWithDataChange("date",
                QueryValue.ofSql("TO_DATE('2018-03-27','yyyy-mm-dd')"),
                QueryValue.ofSql("TO_DATE('2018-10-15','yyyy-mm-dd')"),
                1_522_108_800_000L,
                1_539_561_600_000L);
        streamSchemaChangeMixedWithDataChange("timestamp",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890);
        streamSchemaChangeMixedWithDataChange("timestamp(2)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000 + 130);
        streamSchemaChangeMixedWithDataChange("timestamp(4)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 125500);
        streamSchemaChangeMixedWithDataChange("timestamp(9)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9)),
                LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789,
                LocalDateTime.of(2018, 10, 15, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000 + 123456789);
        streamSchemaChangeMixedWithDataChange("timestamp with time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00")),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamSchemaChangeMixedWithDataChange("timestamp with local time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00")),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamSchemaChangeMixedWithDataChange("interval year to month",
                QueryValue.ofSql("INTERVAL '-3-6' YEAR TO MONTH"),
                QueryValue.ofSql("INTERVAL '-2-5' YEAR TO MONTH"),
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamSchemaChangeMixedWithDataChange("interval day(3) to second(2)",
                QueryValue.ofSql("INTERVAL '-1 2:3:4.56' DAY TO SECOND"),
                QueryValue.ofSql("INTERVAL '-2 4:5:6.21' DAY TO SECOND"),
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @Test
    @FixFor("DBZ-3401")
    @SkipLongRunning
    public void shouldStreamSchemaChangeWithDataChangeTemporalDataTypesAsConnect() throws Exception {
        // Override TemporalPrecisionMode default
        temporalPrecisionMode = TemporalPrecisionMode.CONNECT;

        streamSchemaChangeMixedWithDataChange("date",
                QueryValue.ofSql("TO_DATE('2018-03-27','yyyy-mm-dd')"),
                QueryValue.ofSql("TO_DATE('2018-10-15','yyyy-mm-dd')"),
                Date.from(LocalDate.of(2018, 3, 27).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDate.of(2018, 10, 15).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant()));
        streamSchemaChangeMixedWithDataChange("timestamp",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 789, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 789, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 7890 * 1_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 7890 * 1_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamSchemaChangeMixedWithDataChange("timestamp(2)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 130 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 130 * 1_000_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamSchemaChangeMixedWithDataChange("timestamp(4)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 12545, 5)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 12545, 5)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 125_500 * 1_000).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 125_500 * 1_000).atOffset(ZoneOffset.UTC).toInstant()));
        streamSchemaChangeMixedWithDataChange("timestamp(9)",
                QueryValue.ofSql(toTimestamp(2018, 3, 27, 12, 34, 56, 123456789, 9)),
                QueryValue.ofSql(toTimestamp(2018, 10, 15, 12, 34, 56, 123456789, 9)),
                Date.from(LocalDateTime.of(2018, 3, 27, 12, 34, 56, 123456789).atOffset(ZoneOffset.UTC).toInstant()),
                Date.from(LocalDateTime.of(2018, 10, 15, 12, 34, 56, 123456789).atOffset(ZoneOffset.UTC).toInstant()));
        streamSchemaChangeMixedWithDataChange("timestamp with time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-11:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-11:00")),
                "2018-03-27T01:34:56.007890-11:00",
                "2018-10-15T01:34:56.007890-11:00");
        streamSchemaChangeMixedWithDataChange("timestamp with local time zone",
                QueryValue.ofSql(toTimestampTz(2018, 3, 27, 1, 34, 56, 7890, 6, "-06:00")),
                QueryValue.ofSql(toTimestampTz(2018, 10, 15, 1, 34, 56, 7890, 6, "-06:00")),
                "2018-03-27T07:34:56.007890Z",
                "2018-10-15T07:34:56.007890Z");
        streamSchemaChangeMixedWithDataChange("interval year to month",
                QueryValue.ofSql("INTERVAL '-3-6' YEAR TO MONTH"),
                QueryValue.ofSql("INTERVAL '-2-5' YEAR TO MONTH"),
                -110_451_600_000_000L,
                -76_264_200_000_000L);
        streamSchemaChangeMixedWithDataChange("interval day(3) to second(2)",
                QueryValue.ofSql("INTERVAL '-1 2:3:4.56' DAY TO SECOND"),
                QueryValue.ofSql("INTERVAL '-2 4:5:6.21' DAY TO SECOND"),
                -93_784_560_000L,
                -187_506_210_000L);
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamOfflineSchemaChangeWithExistingLegacySchemaHistory() throws Exception {
        TestHelper.dropTable(connection, "dbz3401");
        try {
            LogInterceptor logInterceptor = new LogInterceptor(BaseSourceTask.class);

            final String columnName = "C1";
            final String columnType = "varchar2(50)";
            final QueryValue insertValue = QueryValue.ofBind("test");
            final QueryValue updateValue = QueryValue.ofBind("updated");
            final String expectedInsert = "test";
            final String expectedUpdate = "updated";

            // create table & stream it
            createAndStreamTable(columnName, columnType);

            // Create schema history
            createSchemaHistoryForDdl(String.format(
                    "CREATE TABLE dbz3401 (id numeric(9,0) primary key, %s %s)", columnName, columnType));

            // Create Offsets
            createOffsetBasedOnCurrentScn();

            Configuration config = configureAndStartConnector(false);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            assertThat(logInterceptor.containsMessage("No previous offsets found"))
                    .as("Existing offsets were not found but expected")
                    .isFalse();

            assertNoRecordsToConsume();

            stopConnector();

            // Do offline actions
            insertRowWithoutCommit(columnName, insertValue, 1);
            connection.commit();
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            insertRowOffline(columnName, insertValue, 2);
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");
            updateRowOffline(columnName, updateValue, 2);
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            connection.execute("DELETE FROM dbz3401 WHERE ID = 2");
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final int expected = 4;
            SourceRecords records = consumeRecordsByTopic(expected);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));

            // Insert (before schema change)
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName)).isEqualTo(expectedInsert);

            // Insert
            after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedInsert);
            assertThat(after.get(columnName + "2")).isEqualTo(expectedInsert);

            // Update
            after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(after.schema().field(columnName + "2")).isNull();

            // Delete
            Struct before = ((Struct) tableRecords.get(3).value()).getStruct(Envelope.FieldName.BEFORE);
            after = ((Struct) tableRecords.get(3).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(before.get(columnName + "2")).isNull();
            assertThat(after).isNull();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    @Test
    @FixFor("DBZ-3401")
    public void shouldStreamSchemaChangeWithExistingLegacySchemaHistory() throws Exception {
        TestHelper.dropTable(connection, "dbz3401");
        try {
            LogInterceptor logInterceptor = new LogInterceptor(BaseSourceTask.class);

            final String columnName = "C1";
            final String columnType = "varchar2(50)";
            final QueryValue insertValue = QueryValue.ofBind("test");
            final QueryValue updateValue = QueryValue.ofBind("updated");
            final String expectedInsert = "test";
            final String expectedUpdate = "updated";

            createAndStreamTable(columnName, columnType);

            // Create schema history
            createSchemaHistoryForDdl(String.format(
                    "CREATE TABLE dbz3401 (id numeric(9,0) primary key, %s %s)", columnName, columnType));

            // Create Offsets
            createOffsetBasedOnCurrentScn();

            configureAndStartConnector(false);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            assertThat(logInterceptor.containsMessage("No previous offsets found"))
                    .as("Existing offsets were not found but expected")
                    .isFalse();

            // insert streaming record
            insertRowWithoutCommit(columnName, insertValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C2 varchar2(50)");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);
            assertThat(after.schema().field("C2")).isNull(); // field was added after insert

            // update streaming record
            updateRowWithoutCommit(columnName, updateValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C3 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(after.get("C2")).isNull();
            assertThat(after.schema().field("C3")).isNull(); // field was added after update

            // delete streaming record
            connection.executeWithoutCommitting("DELETE FROM dbz3401 where id = 1");
            connection.execute("ALTER TABLE dbz3401 add C4 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct before = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(before.get("C2")).isNull();
            assertThat(before.get("C3")).isNull();

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 2);
            // This test case does not use PURGE so that the table gets pushed into the Oracle RECYCLEBIN
            // LogMiner materializes table name as "ORCLPDB1.DEBEZIUM.BIN$<base64>==$0"
            connection.execute("DROP TABLE dbz3401");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            // Now lets test re-creating the table in-flight
            // This should automatically capture the schema object details
            createAndStreamTable(columnName, columnType);

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 3);
            // This test case uses PURGE.
            // LogMiner materializes table name as "ORCLPDB1.UNKNOWN.OBJ# <num>"
            connection.execute("DROP TABLE dbz3401 PURGE");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            stopConnector();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    @Test
    @FixFor("DBZ-8597")
    public void shouldStreamWhenTableHasAnInvisibleGeneratedColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz8597");
        try {
            connection.execute("CREATE TABLE dbz8597 (" +
                    "id numeric(9,0) primary key, " +
                    "val_gen varchar2(50) invisible generated always as (VAL1 || '-' || VAL2), " +
                    "val1 varchar2(10), " +
                    "val2 varchar2(10))");
            TestHelper.streamTable(connection, "dbz8597");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8597")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "hybrid")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz8597 (id,val1,val2) values (1,'a','b')");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ8597");
            assertThat(tableRecords).hasSize(1);

            stopConnector();

            connection.execute("UPDATE dbz8597 SET val2 = 'B1' WHERE id = 1");
            connection.execute("ALTER TABLE dbz8597 add val3 varchar2(10)");
            connection.execute("UPDATE dbz8597 SET val3 = 'C2' WHERE id = 1");

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            records = consumeRecordsByTopic(2);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ8597");
            assertThat(tableRecords).hasSize(2);

            Struct value = ((Struct) tableRecords.get(0).value());
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("VAL1")).isEqualTo("a");
            assertThat(before.get("VAL2")).isEqualTo("b");

            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("VAL1")).isEqualTo("a");
            assertThat(after.get("VAL2")).isEqualTo("B1");

            value = ((Struct) tableRecords.get(1).value());
            before = value.getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get("VAL1")).isEqualTo("a");
            assertThat(before.get("VAL2")).isEqualTo("B1");

            after = value.getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("VAL1")).isEqualTo("a");
            assertThat(after.get("VAL2")).isEqualTo("B1");
            assertThat(after.get("VAL3")).isEqualTo("C2");

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8597");
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static String toTimestamp(int year, int month, int day, int hour, int min, int sec, int nanos, int precision) {
        String nanoSeconds = Strings.justify(Strings.Justify.RIGHT, String.valueOf(nanos), precision, '0');
        String format = "'%04d-%02d-%02d %02d:%02d:%02d.%s', 'yyyy-mm-dd HH24:MI:SS.FF" + precision + "'";
        return String.format("TO_TIMESTAMP(" + format + ")", year, month, day, hour, min, sec, nanoSeconds);
    }

    @SuppressWarnings("SameParameterValue")
    private static String toTimestampTz(int year, int month, int day, int hour, int min, int sec, int nanos, int precision, String tz) {
        String nanoSeconds = Strings.justify(Strings.Justify.RIGHT, String.valueOf(nanos), precision, '0');
        String format = "'%04d-%02d-%02d %02d:%02d:%02d.%s %s', 'yyyy-mm-dd HH24:MI:SS.FF" + precision + " TZH:TZM'";
        return String.format("TO_TIMESTAMP_TZ(" + format + ")", year, month, day, hour, min, sec, nanoSeconds, tz);
    }

    private void streamOfflineSchemaChanges(String columnType, QueryValue insertValue, QueryValue updateValue,
                                            Object expectedInsert, Object expectedUpdate)
            throws Exception {
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, false, false);
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, true, false);
        streamOfflineSchemaChanges(columnType, insertValue, updateValue, expectedInsert, expectedUpdate, true, true);
    }

    @SuppressWarnings("SameParameterValue")
    private void streamSchemaChangeMixedWithDataChange(String columnType, QueryValue insertValue, QueryValue updateValue,
                                                       Object expectedInsert, Object expectedUpdate)
            throws Exception {
        final String columnName = "C1";
        TestHelper.dropTable(connection, "dbz3401");
        try {
            createAndStreamTable(columnName, columnType);

            configureAndStartConnector(false);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // insert streaming record
            insertRowWithoutCommit(columnName, insertValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C2 varchar2(50)");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);
            assertThat(after.schema().field("C2")).isNull(); // field was added after insert

            // update streaming record
            updateRowWithoutCommit(columnName, updateValue, 1);
            // add a new column to trigger a schema change
            connection.execute("ALTER TABLE dbz3401 add C3 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(after.get("C2")).isNull();
            assertThat(after.schema().field("C3")).isNull(); // field was added after update

            // delete streaming record
            connection.executeWithoutCommitting("DELETE FROM dbz3401 where id = 1");
            connection.execute("ALTER TABLE dbz3401 add C4 varchar2(50)");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            Struct before = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.get("ID")).isEqualTo(1);
            assertThat(before.get(columnName.toUpperCase())).isEqualTo(expectedUpdate);
            assertThat(before.get("C2")).isNull();
            assertThat(before.get("C3")).isNull();

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNull();

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 2);
            // This test case does not use PURGE so that the table gets pushed into the Oracle RECYCLEBIN
            // LogMiner materializes table name as "ORCLPDB1.DEBEZIUM.BIN$<base64>==$0"
            connection.execute("DROP TABLE dbz3401");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            // Now lets test re-creating the table in-flight
            // This should automatically capture the schema object details
            createAndStreamTable(columnName, columnType);

            // Perform DML and then DDL (drop table within same scope)
            insertRowWithoutCommit(columnName, insertValue, 3);
            // This test case uses PURGE.
            // LogMiner materializes table name as "ORCLPDB1.UNKNOWN.OBJ# <num>"
            connection.execute("DROP TABLE dbz3401 PURGE");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after).isNotNull();
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get(columnName.toUpperCase())).isEqualTo(expectedInsert);

            stopConnector();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    private void streamOfflineSchemaChanges(String columnType, QueryValue insertValue, QueryValue updateValue,
                                            Object expectedInsert, Object expectedUpdate,
                                            boolean dropTable, boolean dropTableWithPurge)
            throws Exception {
        final String columnName = "C1";

        TestHelper.dropTable(connection, "dbz3401");
        try {
            // create table & stream it
            createAndStreamTable(columnName, columnType);

            // insert snapshot record
            insertRowWithoutCommit(columnName, insertValue, 1);
            connection.commit();

            Configuration config = configureAndStartConnector(false);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));
            assertThat(tableRecords).hasSize(1);

            stopConnector();

            // Do offline actions
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            insertRowOffline(columnName, insertValue, 2);
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");
            updateRowOffline(columnName, updateValue, 2);
            connection.execute("ALTER TABLE dbz3401 ADD " + columnName + "2 " + columnType);
            connection.execute("DELETE FROM dbz3401 WHERE ID = 2");
            connection.execute("ALTER TABLE dbz3401 DROP COLUMN " + columnName + "2");

            if (dropTable) {
                if (dropTableWithPurge) {
                    connection.execute("DROP TABLE dbz3401 PURGE");
                }
                else {
                    TestHelper.dropTable(connection, "dbz3401");
                }
            }

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final int expected = 3;
            records = consumeRecordsByTopic(expected);
            tableRecords = records.recordsForTopic(topicName("DEBEZIUM", "DBZ3401"));

            // Insert
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedInsert);
            assertThat(after.get(columnName + "2")).isEqualTo(expectedInsert);

            // Update
            after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(after.schema().field(columnName + "2")).isNull();

            // Delete
            Struct before = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.BEFORE);
            after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(before.get("ID")).isEqualTo(2);
            assertThat(before.get(columnName)).isEqualTo(expectedUpdate);
            assertThat(before.get(columnName + "2")).isNull();
            assertThat(after).isNull();
        }
        finally {
            // Shutdown the connector explicitly
            stopConnector();

            // drop the table in case of a failure
            TestHelper.dropTable(connection, "dbz3401");

            // cleanup state from multiple invocations
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
            Testing.Files.delete(OFFSET_STORE_PATH);
        }
    }

    private void createOffsetBasedOnCurrentScn() throws Exception {
        final Scn currentScn;
        try (OracleConnection admin = TestHelper.adminConnection()) {
            currentScn = admin.getCurrentScn();
        }

        final Converter keyConverter = KafkaConnectUtil.converterForOffsetStore();
        final Converter valueConverter = KafkaConnectUtil.converterForOffsetStore();

        final Map<String, String> embeddedConfig = TestHelper.defaultConfig().build().asMap(EmbeddedEngineConfig.ALL_FIELDS);
        embeddedConfig.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, keyConverter.getClass().getName());
        embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, valueConverter.getClass().getName());
        System.out.println(embeddedConfig);

        final OffsetBackingStore store = KafkaConnectUtil.fileOffsetBackingStore();
        store.configure(new TestWorkerConfig(embeddedConfig));
        store.start();

        try {
            final Map<String, Object> partition = Map.of("server", TestHelper.SERVER_NAME);
            final Map<String, Object> offsets = Map.of(
                    "snapshot", true,
                    "scn", currentScn.toString(),
                    "snapshot_completed", true);

            final OffsetStorageWriter writer = new OffsetStorageWriter(store, "testing-connector", keyConverter, valueConverter);
            writer.offset(partition, offsets);

            writer.beginFlush();

            Future<Void> flush = writer.doFlush((error, result) -> {
                // do nothing
            });
            assertThat(flush).isNotNull();

            // wait for flush
            flush.get();
        }
        finally {
            store.stop();
        }
    }

    private void createSchemaHistoryForDdl(String ddlText) {
        final SchemaHistory schemaHistory = new FileSchemaHistory();
        schemaHistory.configure(Configuration.create()
                .with(FileSchemaHistory.FILE_PATH, TestHelper.SCHEMA_HISTORY_PATH.toString())
                .build(),
                null,
                SchemaHistoryMetrics.NOOP,
                true);
        schemaHistory.start();

        final String databaseName = TestHelper.getDatabaseName().toUpperCase();
        final String schemaName = TestHelper.SCHEMA_USER.toUpperCase();

        final Map<String, Object> source = Collect.linkMapOf("server", TestHelper.SERVER_NAME);
        final Map<String, Object> position = Collect.linkMapOf(
                "commit_scn", "1001:1:",
                "snapshot_scn", "1001",
                "scn", "1001",
                "snapshot_completed", true);

        OracleDdlParser parser = new OracleDdlParser();
        DdlChanges ddlChanges = parser.getDdlChanges();
        Tables tables = new Tables();

        ddlChanges.reset();
        parser.setCurrentDatabase(databaseName);
        parser.setCurrentSchema(schemaName);
        parser.parse(ddlText, tables);

        ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
            events.forEach(event -> {
                if (event instanceof TableCreatedEvent) {
                    final TableCreatedEvent createEvent = (TableCreatedEvent) event;
                    final Table table = tables.forTable(createEvent.tableId());
                    final TableChanges changes = new TableChanges().create(table);
                    schemaHistory.record(source, position, databaseName, schemaName, ddlText, changes, Instant.now());
                }
            });
        });
    }

    private Struct varScaleDecimal(String value) {
        return fromLogical(VariableScaleDecimal.builder().optional().build(), new BigDecimal(value));
    }

    @SuppressWarnings("SameParameterValue")
    private static String topicName(String schema, String table) {
        return TestHelper.SERVER_NAME + "." + schema + "." + table;
    }

    @SuppressWarnings("SameParameterValue")
    private void createAndStreamTable(String columnName, String columnType) throws SQLException {
        // create table & stream it
        connection.execute(String.format("CREATE TABLE dbz3401 (id numeric(9,0) not null primary key, %s %s)",
                columnName, columnType));
        TestHelper.streamTable(connection, "dbz3401");
    }

    @SuppressWarnings("SameParameterValue")
    private Configuration configureAndStartConnector(boolean lobEnabled) {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3401")
                // we explicitly want to target this strategy
                .with(OracleConnectorConfig.LOB_ENABLED, Boolean.toString(lobEnabled))
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "hybrid")
                .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(OracleConnectorConfig.DECIMAL_HANDLING_MODE, decimalHandlingMode.getValue())
                .with(OracleConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode.getValue())
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        return config;
    }

    @SuppressWarnings("SameParameterValue")
    private void insertRowWithoutCommit(String columnName, QueryValue insertValue, Integer id) throws SQLException {
        if (insertValue.isSqlFragment()) {
            connection.executeWithoutCommitting(String.format("INSERT INTO dbz3401 (id,%s) values (%d,%s)",
                    columnName, id, insertValue.getValue()));
        }
        else {
            connection.prepareUpdate(
                    String.format("INSERT INTO dbz3401 (id,%s) values (%d,?)", columnName, id),
                    p -> p.setObject(1, insertValue.getValue()));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void updateRowWithoutCommit(String columnName, QueryValue updateValue, Integer id) throws SQLException {
        if (updateValue.isSqlFragment()) {
            connection.execute(String.format("UPDATE dbz3401 set %s=%s WHERE id=%d", columnName, updateValue.getValue(), id));
        }
        else {
            connection.prepareUpdate(
                    String.format("UPDATE dbz3401 set %s=? where id=%d", columnName, id),
                    p -> p.setObject(1, updateValue.getValue()));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void insertRowOffline(String columnName, QueryValue insertValue, Integer id) throws SQLException {
        if (insertValue.isSqlFragment()) {
            connection.execute(String.format("INSERT INTO dbz3401 (id,%s,%s2) values (%d,%s,%s)",
                    columnName, columnName, id, insertValue.getValue(), insertValue.getValue()));
        }
        else {
            connection.prepareUpdate(
                    String.format("INSERT INTO dbz3401 (id,%s,%s2) values (%d,?,?)", columnName, columnName, id),
                    p -> {
                        p.setObject(1, insertValue.getValue());
                        p.setObject(2, insertValue.getValue());
                    });
            connection.commit();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void updateRowOffline(String columnName, QueryValue updateValue, Integer id) throws SQLException {
        if (updateValue.isSqlFragment()) {
            connection.execute(String.format("UPDATE dbz3401 SET %s=%s WHERE id=%d", columnName, updateValue.getValue(), id));
        }
        else {
            connection.prepareUpdate(
                    String.format("UPDATE dbz3401 SET %s=? where id=%d", columnName, id),
                    p -> p.setObject(1, updateValue.getValue()));
            connection.commit();
        }
    }

    /**
     * Contract for passing different types of values that require different query bindings.
     */
    private interface QueryValue {
        /**
         * Return {@code true} if the value should be bound as a SQL fragment
         */
        boolean isSqlFragment();

        /**
         * Return the value of the binding, can be {@code null}
         */
        Object getValue();

        /**
         * Creates a {@link SqlFragmentQueryValue} that binds the value as an inline SQL fragment
         * @param value the value to be inlined
         * @return the query value
         */
        static QueryValue ofSql(String value) {
            return new SqlFragmentQueryValue(value);
        }

        /**
         * Creates a {@link BindQueryValue} that binds the value using JDBC bind variables
         * @param value the value to be bound
         * @return the query value
         */
        static QueryValue ofBind(Object value) {
            return new BindQueryValue(value);
        }
    }

    /**
     * Binds the supplied value as line SQL fragment in the query
     */
    private static class SqlFragmentQueryValue implements QueryValue {
        private final String value;

        SqlFragmentQueryValue(String value) {
            this.value = value;
        }

        @Override
        public boolean isSqlFragment() {
            return true;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    /**
     * Binds the provided value as a JDBC bind variable
     */
    private static class BindQueryValue implements QueryValue {
        private final Object value;

        BindQueryValue(Object value) {
            this.value = value;
        }

        @Override
        public boolean isSqlFragment() {
            return false;
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    // Taken from EmbeddedEngine
    protected static class TestWorkerConfig extends WorkerConfig {
        private static final ConfigDef CONFIG;

        static {
            ConfigDef config = baseConfigDef();
            Field.group(config, "file", EmbeddedEngineConfig.OFFSET_STORAGE_FILE_FILENAME);
            Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_TOPIC);
            Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_PARTITIONS);
            Field.group(config, "kafka", EmbeddedEngineConfig.OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR);
            CONFIG = config;
        }

        protected TestWorkerConfig(Map<String, String> props) {
            super(CONFIG, props);
        }
    }
}

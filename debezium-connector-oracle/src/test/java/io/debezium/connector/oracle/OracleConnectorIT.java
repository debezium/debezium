/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.util.TestHelper.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_NAME_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.TYPE_SCALE_PARAMETER_KEY;
import static io.debezium.connector.oracle.util.TestHelper.defaultConfig;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.OracleConnectorConfig.TransactionSnapshotBoundaryMode;
import io.debezium.connector.oracle.junit.SkipOnDatabaseOption;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnDatabaseOptionRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingAdapter;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * Integration test for the Debezium Oracle connector.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorIT extends AbstractAsyncEngineConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorIT.class);

    private static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    private static final String ERROR_PROCESSING_FAIL_MESSAGE = "Oracle LogMiner is unable to re-construct the SQL for '";
    private static final String ERROR_PROCESSING_WARN_MESSAGE = "cannot be parsed. This event will be ignored and skipped.";

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();
    @Rule
    public final TestRule skipOptionRule = new SkipTestDependingOnDatabaseOptionRule();
    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();

        // Several tests in this class expect the existence of the following tables and only these
        // tables; however if other tests fail or end prematurely, they could taint the number of
        // tables.
        TestHelper.dropAllTables();

        TestHelper.dropTable(connection, "debezium.customer");
        TestHelper.dropTable(connection, "debezium.masked_hashed_column_table");
        TestHelper.dropTable(connection, "debezium.truncated_column_table");
        TestHelper.dropTable(connection, "debezium.dt_table");

        String ddl = "create table debezium.customer (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl2 = "create table debezium.masked_hashed_column_table (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(255), " +
                "  name2 varchar2(255), " +
                "  name3 varchar2(20)," +
                "  primary key (id)" +
                ")";

        connection.execute(ddl2);
        connection.execute("GRANT SELECT ON debezium.masked_hashed_column_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.masked_hashed_column_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl3 = "create table debezium.truncated_column_table (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(20), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl3);
        connection.execute("GRANT SELECT ON debezium.truncated_column_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.truncated_column_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        String ddl4 = "create table dt_table (" +
                "  id numeric(9,0) not null, " +
                "  c1 int, " +
                "  c2 int, " +
                "  c3a numeric(5,2), " +
                "  c3b varchar(128), " +
                "  f1 float(10), " +
                "  f2 decimal(8,4), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl4);
        connection.execute("GRANT SELECT ON debezium.dt_table to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.dt_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.customer2");
            TestHelper.dropTable(connection, "customer");
            TestHelper.dropTable(connection, "masked_hashed_column_table");
            TestHelper.dropTable(connection, "truncated_column_table");
            TestHelper.dropTable(connection, "dt_table");
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        TestHelper.dropTable(connection, "debezium.dbz800a");
        TestHelper.dropTable(connection, "debezium.dbz800b");
        connection.execute("delete from debezium.customer");
        connection.execute("delete from debezium.masked_hashed_column_table");
        connection.execute("delete from debezium.truncated_column_table");
        connection.execute("delete from debezium.dt_table");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-2452")
    public void shouldSnapshotAndStreamWithHyphenedTableName() throws Exception {
        TestHelper.dropTable(connection, "debezium.\"my-table\"");
        try {
            String ddl = "create table \"my-table\" (" +
                    " id numeric(9,0) not null, " +
                    " c1 int, " +
                    " c2 varchar(128), " +
                    " primary key (id))";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.\"my-table\" to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.\"my-table\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            connection.execute("INSERT INTO debezium.\"my-table\" VALUES (1, 25, 'Test')");
            connection.execute("COMMIT");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.MY-TABLE")
                    // DBZ-5541 changed default from "avro" to "none", this test explicitly requires avro
                    // since the VerifyRecord class still validates avro and the table name used is not
                    // compatible with avro naming conventions.
                    .with(OracleConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, "avro")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.\"my-table\" VALUES (2, 50, 'Test2')");
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(2);
            List<SourceRecord> hyphenatedTableRecords = records.recordsForTopic("server1.DEBEZIUM.my-table");
            assertThat(hyphenatedTableRecords).hasSize(2);

            // read
            SourceRecord record1 = hyphenatedTableRecords.get(0);
            VerifyRecord.isValidRead(record1, "ID", 1);
            Struct after1 = (Struct) ((Struct) record1.value()).get(AFTER);
            assertThat(after1.get("ID")).isEqualTo(1);
            assertThat(after1.get("C1")).isEqualTo(BigDecimal.valueOf(25L));
            assertThat(after1.get("C2")).isEqualTo("Test");
            assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
            assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

            // insert
            SourceRecord record2 = hyphenatedTableRecords.get(1);
            VerifyRecord.isValidInsert(record2, "ID", 2);
            Struct after2 = (Struct) ((Struct) record2.value()).get(AFTER);
            assertThat(after2.get("ID")).isEqualTo(2);
            assertThat(after2.get("C1")).isEqualTo(BigDecimal.valueOf(50L));
            assertThat(after2.get("C2")).isEqualTo("Test2");
        }
        finally {
            TestHelper.dropTable(connection, "debezium.\"my-table\"");
        }
    }

    @Test
    public void shouldTakeSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("first");

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isNull();

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        source = (Struct) ((Struct) record2.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
    }

    @Test
    @FixFor("DBZ-6276")
    @Ignore("Requires database to be configured without ARCHIVELOG_MODE enabled; which conflicts with dbz-oracle images")
    public void shouldSkipCheckingArchiveLogIfNoCdc() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.SKIP)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        LogInterceptor logInterceptor = new LogInterceptor(OracleConnectorTask.class);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        stopConnector();

        assertThat(logInterceptor.containsWarnMessage("Failed the archive log check but continuing as redo log isn't strictly required")).isTrue();
    }

    @Test
    public void shouldContinueWithStreamingAfterSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        continueStreamingAfterSnapshot(config);
    }

    private void continueStreamingAfterSnapshot(Configuration config) throws Exception {
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("first");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        source = (Struct) ((Struct) record2.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");

        expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Brian', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        SourceRecord record3 = testTableRecords.get(0);
        VerifyRecord.isValidInsert(record3, "ID", 3);
        after = (Struct) ((Struct) record3.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);

        assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
        assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();

        source = (Struct) ((Struct) record3.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("false");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
    }

    @Test
    @FixFor("DBZ-1223")
    public void shouldStreamTransaction() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // read
        SourceRecord record1 = testTableRecords.get(0);
        VerifyRecord.isValidRead(record1, "ID", 1);
        Struct after = (Struct) ((Struct) record1.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);

        Struct source = (Struct) ((Struct) record1.value()).get("source");
        assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("first");
        assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
        assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
        assertThat(source.get(SourceInfo.TXID_KEY)).isNull();
        assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();

        assertThat(record1.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record1.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(false);

        SourceRecord record2 = testTableRecords.get(1);
        VerifyRecord.isValidRead(record2, "ID", 2);
        after = (Struct) ((Struct) record2.value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);

        assertThat(record2.sourceOffset().get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(SnapshotType.INITIAL.toString());
        assertThat(record2.sourceOffset().get(SNAPSHOT_COMPLETED_KEY)).isEqualTo(true);

        expectedRecordCount = 30;
        connection.setAutoCommit(false);
        sendTxBatch(config, expectedRecordCount, 100);
        sendTxBatch(config, expectedRecordCount, 200);
    }

    private void sendTxBatch(Configuration config, int expectedRecordCount, int offset) throws SQLException, InterruptedException {
        boolean isAutoCommit = false;
        if (connection.connection().getAutoCommit()) {
            isAutoCommit = true;
            connection.connection().setAutoCommit(false);
        }
        for (int i = offset; i < expectedRecordCount + offset; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();
        if (isAutoCommit) {
            connection.connection().setAutoCommit(true);
        }

        assertTxBatch(config, expectedRecordCount, offset);
    }

    private void assertTxBatch(Configuration config, int expectedRecordCount, int offset) throws InterruptedException {
        SourceRecords records;
        List<SourceRecord> testTableRecords;
        Struct after;
        Struct source;
        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);
        final ConnectorAdapter adapter = TestHelper.getAdapter(config);

        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record3 = testTableRecords.get(i);
            VerifyRecord.isValidInsert(record3, "ID", i + offset);
            after = (Struct) ((Struct) record3.value()).get("after");
            assertThat(after.get("ID")).isEqualTo(i + offset);

            assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
            assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();

            if (ConnectorAdapter.XSTREAM == adapter) {
                assertThat(record3.sourceOffset().containsKey(SourceInfo.LCR_POSITION_KEY)).isTrue();
                assertThat(record3.sourceOffset().containsKey(SourceInfo.SCN_KEY)).isFalse();
            }

            source = (Struct) ((Struct) record3.value()).get("source");
            assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo("false");
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
            if (ConnectorAdapter.XSTREAM == adapter) {
                assertThat(source.get(SourceInfo.LCR_POSITION_KEY)).isNotNull();
            }

            assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
            assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
        }
    }

    @Test
    public void shouldStreamAfterRestart() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        expectedRecordCount = 30;
        connection.setAutoCommit(false);
        sendTxBatch(config, expectedRecordCount, 100);
        sendTxBatch(config, expectedRecordCount, 200);

        stopConnector();
        final int OFFSET = 300;
        for (int i = OFFSET; i < expectedRecordCount + OFFSET; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        assertTxBatch(config, expectedRecordCount, 300);
        sendTxBatch(config, expectedRecordCount, 400);
        sendTxBatch(config, expectedRecordCount, 500);
    }

    @Test
    public void shouldStreamAfterRestartAfterSnapshot() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        // Testing.Print.enable();
        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Bruce', 2345.67, null)");
        connection.execute("COMMIT");
        expectedRecordCount += 2;

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);
        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        stopConnector();

        connection.setAutoCommit(false);
        final int OFFSET = 100;
        for (int i = OFFSET; i < expectedRecordCount + OFFSET; i++) {
            connection.executeWithoutCommitting(String.format("INSERT INTO debezium.customer VALUES (%s, 'Brian%s', 2345.67, null)", i, i));
        }
        connection.connection().commit();

        try {
            connection.setAutoCommit(true);

            Testing.print("=== Starting connector second time ===");
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            assertTxBatch(config, expectedRecordCount, 100);
            sendTxBatch(config, expectedRecordCount, 200);
        }
        finally {
            connection.setAutoCommit(false);
        }
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        expectedRecordCount += 1;

        connection.execute("UPDATE debezium.customer SET name = 'Bruce', score = 2345.67, registered = TO_DATE('2018-03-23', 'yyyy-mm-dd') WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        connection.execute("UPDATE debezium.customer SET id = 2 WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 3; // deletion + tombstone + insert with new id

        connection.execute("DELETE debezium.customer WHERE id = 2");
        connection.execute("COMMIT");
        expectedRecordCount += 2; // deletion + tombstone

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        Map<String, ?> offset = testTableRecords.get(0).sourceOffset();
        assertThat(offset.get(SourceInfo.SNAPSHOT_KEY)).isNull();
        assertThat(offset.get("snapshot_completed")).isNull();

        // update
        VerifyRecord.isValidUpdate(testTableRecords.get(1), "ID", 1);
        Struct before = (Struct) ((Struct) testTableRecords.get(1).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        after = (Struct) ((Struct) testTableRecords.get(1).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // update of PK
        VerifyRecord.isValidDelete(testTableRecords.get(2), "ID", 1);
        before = (Struct) ((Struct) testTableRecords.get(2).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(3));

        VerifyRecord.isValidInsert(testTableRecords.get(4), "ID", 2);
        after = (Struct) ((Struct) testTableRecords.get(4).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Bruce");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        // delete
        VerifyRecord.isValidDelete(testTableRecords.get(5), "ID", 2);
        before = (Struct) ((Struct) testTableRecords.get(5).value()).get("before");
        assertThat(before.get("ID")).isEqualTo(2);
        assertThat(before.get("NAME")).isEqualTo("Bruce");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(2345.67));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 3, 23, 0, 0, 0)));

        VerifyRecord.isValidTombstone(testTableRecords.get(6));
    }

    @Test
    @FixFor("DBZ-835")
    public void deleteWithoutTombstone() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        int expectedRecordCount = 0;
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        connection.execute("DELETE debezium.customer WHERE id = 1");
        connection.execute("COMMIT");
        expectedRecordCount += 1; // deletion, no tombstone

        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");
        expectedRecordCount += 1;

        SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // delete
        VerifyRecord.isValidDelete(testTableRecords.get(1), "ID", 1);
        final Struct before = ((Struct) testTableRecords.get(1).value()).getStruct("before");
        assertThat(before.get("ID")).isEqualTo(1);
        assertThat(before.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(before.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(before.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));

        VerifyRecord.isValidInsert(testTableRecords.get(2), "ID", 2);
    }

    @Test
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");
        try {
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER2")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            String ddl = "create table debezium.customer2 (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar2(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")";

            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.customer2");

            connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER2");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 2);
            Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
            assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
            assertThat(after.get("REGISTERED")).isEqualTo(toMicroSecondsSinceEpoch(LocalDateTime.of(2018, 2, 22, 0, 0, 0)));
        }
        finally {
            TestHelper.dropTable(connection, "debezium.customer2");
        }
    }

    @Test
    @FixFor("DBZ-800")
    public void shouldReceiveHeartbeatAlsoWhenChangingTableIncludeListTables() throws Exception {
        TestHelper.dropTable(connection, "debezium.dbz800a");
        TestHelper.dropTable(connection, "debezium.dbz800b");

        // the low heartbeat interval should make sure that a heartbeat message is emitted after each change record
        // received from Postgres
        Configuration config = TestHelper.defaultConfig()
                .with(Heartbeat.HEARTBEAT_INTERVAL, "1")
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ800B")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("CREATE TABLE debezium.dbz800a (id NUMBER(9) NOT NULL, aaa VARCHAR2(100), PRIMARY KEY (id) )");
        connection.execute("CREATE TABLE debezium.dbz800b (id NUMBER(9) NOT NULL, bbb VARCHAR2(100), PRIMARY KEY (id) )");
        connection.execute("INSERT INTO debezium.dbz800a VALUES (1, 'AAA')");
        connection.execute("INSERT INTO debezium.dbz800b VALUES (2, 'BBB')");
        connection.execute("COMMIT");

        // The number of heartbeat events may vary depending on how fast the events are seen in
        // LogMiner, so to compensate for the varied number of heartbeats that might be emitted
        // the following waits until we have seen the DBZ800B event before returning.
        final AtomicReference<SourceRecords> records = new AtomicReference<>();
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> {
                    if (records.get() == null) {
                        records.set(consumeRecordsByTopic(1));
                    }
                    else {
                        consumeRecordsByTopic(1).allRecordsInOrder().forEach(records.get()::add);
                    }
                    return records.get().recordsForTopic("server1.DEBEZIUM.DBZ800B") != null;
                });

        List<SourceRecord> heartbeats = records.get().recordsForTopic("__debezium-heartbeat.server1");
        List<SourceRecord> tableA = records.get().recordsForTopic("server1.DEBEZIUM.DBZ800A");
        List<SourceRecord> tableB = records.get().recordsForTopic("server1.DEBEZIUM.DBZ800B");

        // there should be at least one heartbeat, no events for DBZ800A and one for DBZ800B
        assertThat(heartbeats).isNotEmpty();
        assertThat(tableA).isNull();
        assertThat(tableB).hasSize(1);

        VerifyRecord.isValidInsert(tableB.get(0), "ID", 2);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumnsWithDatabaseName() throws Exception {
        shouldConsumeEventsWithMaskedAndTruncatedColumns(true);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldConsumeEventsWithMaskedAndTruncatedColumnsWithoutDatabaseName() throws Exception {
        shouldConsumeEventsWithMaskedAndTruncatedColumns(false);
    }

    public void shouldConsumeEventsWithMaskedAndTruncatedColumns(boolean useDatabaseName) throws Exception {
        final Configuration config;
        if (useDatabaseName) {
            final String dbName = TestHelper.getDatabaseName();
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with("column.mask.with.12.chars", dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME")
                    .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K",
                            dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME2," + dbName + ".DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME3")
                    .with("column.truncate.to.4.chars", dbName + ".DEBEZIUM.TRUNCATED_COLUMN_TABLE.NAME")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with("column.mask.with.12.chars", "DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME")
                    .with("column.mask.hash.SHA-256.with.salt.CzQMA0cB5K", "DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME2,DEBEZIUM.MASKED_HASHED_COLUMN_TABLE.NAME3")
                    .with("column.truncate.to.4.chars", "DEBEZIUM.TRUNCATED_COLUMN_TABLE.NAME")
                    .build();
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.masked_hashed_column_table (id, name, name2, name3) VALUES (10, 'some_name', 'test', 'test')");
        connection.execute("INSERT INTO debezium.truncated_column_table VALUES(11, 'some_name')");
        connection.execute("COMMIT");

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("server1.DEBEZIUM.MASKED_HASHED_COLUMN_TABLE");
        final List<SourceRecord> tableB = records.recordsForTopic("server1.DEBEZIUM.TRUNCATED_COLUMN_TABLE");

        assertThat(tableA).hasSize(1);
        SourceRecord record = tableA.get(0);
        VerifyRecord.isValidInsert(record, "ID", 10);

        Struct value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            Struct after = value.getStruct("after");
            assertThat(after.getString("NAME")).isEqualTo("************");
            assertThat(after.getString("NAME2")).isEqualTo("8e68c68edbbac316dfe2f6ada6b0d2d3e2002b487a985d4b7c7c82dd83b0f4d7");
            assertThat(after.getString("NAME3")).isEqualTo("8e68c68edbbac316dfe2");
        }

        assertThat(tableB).hasSize(1);
        record = tableB.get(0);
        VerifyRecord.isValidInsert(record, "ID", 11);

        value = (Struct) record.value();
        if (value.getStruct("after") != null) {
            assertThat(value.getStruct("after").getString("NAME")).isEqualTo("some");
        }

        stopConnector();
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldRewriteIdentityKeyWithDatabaseName() throws Exception {
        shouldRewriteIdentityKey(true);
    }

    @Test
    @FixFor("DBZ-775")
    public void shouldRewriteIdentityKeyWithoutDatabaseName() throws Exception {
        shouldRewriteIdentityKey(false);
    }

    private void shouldRewriteIdentityKey(boolean useDatabaseName) throws Exception {
        final Configuration config;
        if (useDatabaseName) {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "(.*).debezium.customer:id,name")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "debezium.customer:id,name")
                    .build();
        }

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Nest', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(recordsForTopic.get(0).key()).isNotNull();
        Struct key = (Struct) recordsForTopic.get(0).key();
        assertThat(key.get("ID")).isNotNull();
        assertThat(key.get("NAME")).isNotNull();

        stopConnector();
    }

    @Test
    @FixFor({ "DBZ-1916", "DBZ-1830" })
    public void shouldPropagateSourceTypeByDatatype() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with("datatype.propagate.source.type", ".+\\.NUMBER,.+\\.VARCHAR2,.+\\.FLOAT")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.dt_table (id,c1,c2,c3a,c3b,f1,f2) values (1,123,456,789.01,'test',1.228,234.56)");
        connection.execute("COMMIT");

        final SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DT_TABLE");
        assertThat(recordsForTopic).hasSize(1);

        final Field before = recordsForTopic.get(0).valueSchema().field("before");

        assertThat(before.schema().field("ID").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "9"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C1").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "38"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C2").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "38"),
                entry(TYPE_SCALE_PARAMETER_KEY, "0"));

        assertThat(before.schema().field("C3A").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "5"),
                entry(TYPE_SCALE_PARAMETER_KEY, "2"));

        assertThat(before.schema().field("C3B").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "VARCHAR2"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "128"));

        assertThat(before.schema().field("F2").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "NUMBER"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "8"),
                entry(TYPE_SCALE_PARAMETER_KEY, "4"));

        assertThat(before.schema().field("F1").schema().parameters()).contains(
                entry(TYPE_NAME_PARAMETER_KEY, "FLOAT"),
                entry(TYPE_LENGTH_PARAMETER_KEY, "10"));
    }

    @Test
    @FixFor({ "DBZ-4385" })
    public void shouldTruncate() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.truncate_ddl");

        try {
            // complex ddl
            final String ddl = "create table debezium.truncate_ddl (" +
                    "id NUMERIC(6), " +
                    "name VARCHAR(100), " +
                    "primary key(id))";

            // create table
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.truncate_ddl");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.truncate_ddl (id, name) values (1, 'Acme')");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.TRUNCATE_DDL")
                    .with(OracleConnectorConfig.SKIPPED_OPERATIONS, "none") // do not skip truncates
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.TRUNCATE_DDL")).hasSize(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // truncate statement
            connection.execute("TRUNCATE TABLE debezium.truncate_ddl");

            SourceRecords streamingRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = streamingRecords.recordsForTopic("server1.DEBEZIUM.TRUNCATE_DDL");
            assertThat(records).hasSize(1);
            String op = ((Struct) records.get(0).value()).getString("op");
            assertThat(op).isEqualTo("t");

            // verify record after truncate
            connection.execute("INSERT INTO debezium.truncate_ddl (id, name) values (2, 'Roadrunner')");
            connection.commit();

            streamingRecords = consumeRecordsByTopic(1);
            records = streamingRecords.recordsForTopic("server1.DEBEZIUM.TRUNCATE_DDL");
            assertThat(records).hasSize(1);
            op = ((Struct) records.get(0).value()).getString("op");
            assertThat(op).isEqualTo("c");
        }
        finally {
            TestHelper.dropTable(connection, "debezium.truncate_ddl");
        }
    }

    @Test
    @FixFor({ "DBZ-4385" })
    public void shouldNotTruncateWhenSkipped() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.truncate_ddl");

        try {
            // complex ddl
            final String ddl = "create table debezium.truncate_ddl (" +
                    "id NUMERIC(6), " +
                    "name VARCHAR(100), " +
                    "primary key(id))";

            // create table
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.truncate_ddl");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.truncate_ddl (id, name) values (1, 'Acme')");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.TRUNCATE_DDL")
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.TRUNCATE_DDL")).hasSize(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // truncate statement
            connection.execute("TRUNCATE TABLE debezium.truncate_ddl");
            // Nothing happens, so nothing to verify either.

            // verify record after truncate
            connection.execute("INSERT INTO debezium.truncate_ddl (id, name) values (2, 'Roadrunner')");
            connection.commit();

            SourceRecords streamingRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = streamingRecords.recordsForTopic("server1.DEBEZIUM.TRUNCATE_DDL");
            assertThat(records).hasSize(1);
            String op = ((Struct) records.get(0).value()).getString("op");
            assertThat(op).isEqualTo("c");
        }
        finally {
            TestHelper.dropTable(connection, "debezium.truncate_ddl");
        }
    }

    @FixFor("DBZ-1539")
    public void shouldHandleIntervalTypesAsInt64() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.interval");

        try {
            // complex ddl
            final String ddl = "create table debezium.interval (" +
                    " id numeric(6) constraint interval_id_nn not null, " +
                    " intYM interval year to month," +
                    " intYM2 interval year(9) to month," + // max precision
                    " intDS interval day to second, " +
                    " intDS2 interval day(9) to second(9), " + // max precision
                    " constraint interval_pk primary key(id)" +
                    ")";

            // create table
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.interval");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (1, INTERVAL '2' YEAR, INTERVAL '555-4' YEAR(3) TO MONTH, "
                    + "INTERVAL '3' DAY, INTERVAL '111 10:09:08.555444333' DAY(3) TO SECOND(9))");
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (2, INTERVAL '0' YEAR, INTERVAL '0' MONTH, "
                    + "INTERVAL '0' DAY, INTERVAL '0' SECOND)");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.INTERVAL")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify record generated during snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(2);
            assertThat(snapshotRecords.allRecordsInOrder()).hasSize(2);
            assertThat(snapshotRecords.topics()).contains("server1.DEBEZIUM.INTERVAL");

            List<SourceRecord> records = snapshotRecords.recordsForTopic("server1.DEBEZIUM.INTERVAL");
            assertThat(records).hasSize(2);

            Struct after = ((Struct) records.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.getInt64("INTYM")).isEqualTo(63115200000000L);
            assertThat(after.getInt64("INTYM2")).isEqualTo(17524987200000000L);
            assertThat(after.getInt64("INTDS")).isEqualTo(259200000000L);
            assertThat(after.getInt64("INTDS2")).isEqualTo(9627503444333L);

            after = ((Struct) records.get(1).value()).getStruct(AFTER);
            assertThat(after.getInt64("INTYM")).isEqualTo(0L);
            assertThat(after.getInt64("INTYM2")).isEqualTo(0L);
            assertThat(after.getInt64("INTDS")).isEqualTo(0L);
            assertThat(after.getInt64("INTDS2")).isEqualTo(0L);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (3, INTERVAL '2' YEAR, INTERVAL '555-4' YEAR(3) TO MONTH, "
                    + "INTERVAL '3' DAY, INTERVAL '111 10:09:08.555444333' DAY(3) TO SECOND(9))");
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (4, INTERVAL '0' YEAR, INTERVAL '0' MONTH, "
                    + "INTERVAL '0' DAY, INTERVAL '0' SECOND)");
            connection.commit();

            // Verify record generated during streaming
            final SourceRecords streamingRecords = consumeRecordsByTopic(2);
            assertThat(streamingRecords.allRecordsInOrder()).hasSize(2);
            assertThat(streamingRecords.topics()).contains("server1.DEBEZIUM.INTERVAL");

            records = streamingRecords.recordsForTopic("server1.DEBEZIUM.INTERVAL");
            assertThat(records).hasSize(2);

            after = ((Struct) records.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.getInt64("INTYM")).isEqualTo(63115200000000L);
            assertThat(after.getInt64("INTYM2")).isEqualTo(17524987200000000L);
            assertThat(after.getInt64("INTDS")).isEqualTo(259200000000L);
            assertThat(after.getInt64("INTDS2")).isEqualTo(9627503444333L);

            after = ((Struct) records.get(1).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            assertThat(after.getInt64("INTYM")).isEqualTo(0L);
            assertThat(after.getInt64("INTYM2")).isEqualTo(0L);
            assertThat(after.getInt64("INTDS")).isEqualTo(0L);
            assertThat(after.getInt64("INTDS2")).isEqualTo(0L);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "debezium.interval");
        }
    }

    @Test
    @FixFor("DBZ-1539")
    public void shouldHandleIntervalTypesAsString() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.interval");

        try {
            // complex ddl
            final String ddl = "create table debezium.interval (" +
                    " id numeric(6) constraint interval_id_nn not null, " +
                    " intYM interval year to month," +
                    " intYM2 interval year(9) to month," + // max precision
                    " intDS interval day to second, " +
                    " intDS2 interval day(9) to second(9), " + // max precision
                    " constraint interval_pk primary key(id)" +
                    ")";

            // create table
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.interval");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (1, INTERVAL '2' YEAR, INTERVAL '555-4' YEAR(3) TO MONTH, "
                    + "INTERVAL '3' DAY, INTERVAL '111 10:09:08.555444333' DAY(3) TO SECOND(9))");
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (2, INTERVAL '0' YEAR, INTERVAL '0' MONTH, "
                    + "INTERVAL '0' DAY, INTERVAL '0' SECOND)");
            // DBZ-6513 negative intervals
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (3, INTERVAL '-1' YEAR, INTERVAL '-1' MONTH, "
                    + "INTERVAL '-1' DAY, INTERVAL '-7 5:12:10.0123' DAY(1) TO SECOND)");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.INTERVAL")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .with(OracleConnectorConfig.INTERVAL_HANDLING_MODE,
                            OracleConnectorConfig.IntervalHandlingMode.STRING.getValue())
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify record generated during snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(3);
            assertThat(snapshotRecords.allRecordsInOrder()).hasSize(3);
            assertThat(snapshotRecords.topics()).contains("server1.DEBEZIUM.INTERVAL");

            List<SourceRecord> records = snapshotRecords.recordsForTopic("server1.DEBEZIUM.INTERVAL");
            assertThat(records).hasSize(3);

            Struct after = ((Struct) records.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.getString("INTYM")).isEqualTo("P2Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P555Y4M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M3DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M111DT10H9M563.444333S");

            after = ((Struct) records.get(1).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.getString("INTYM")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M0DT0H0M0S");

            after = ((Struct) records.get(2).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.getString("INTYM")).isEqualTo("P-1Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P0Y-1M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M-1DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M-7DT-5H-12M-10.0123S");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (4, INTERVAL '2' YEAR, INTERVAL '555-4' YEAR(3) TO MONTH, "
                    + "INTERVAL '3' DAY, INTERVAL '111 10:09:08.555444333' DAY(3) TO SECOND(9))");
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (5, INTERVAL '0' YEAR, INTERVAL '0' MONTH, "
                    + "INTERVAL '0' DAY, INTERVAL '0' SECOND)");
            // DBZ-6513 negative intervals
            connection.execute("INSERT INTO debezium.interval (id, intYM, intYM2, intDS, intDS2) "
                    + "values (6, INTERVAL '-1' YEAR, INTERVAL '-1' MONTH, "
                    + "INTERVAL '-1' DAY, INTERVAL '-7 5:12:10.0123' DAY(1) TO SECOND)");
            connection.commit();

            // Verify record generated during streaming
            final SourceRecords streamingRecords = consumeRecordsByTopic(3);
            assertThat(streamingRecords.allRecordsInOrder()).hasSize(3);
            assertThat(streamingRecords.topics()).contains("server1.DEBEZIUM.INTERVAL");

            records = streamingRecords.recordsForTopic("server1.DEBEZIUM.INTERVAL");
            assertThat(records).hasSize(3);

            after = ((Struct) records.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            assertThat(after.getString("INTYM")).isEqualTo("P2Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P555Y4M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M3DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M111DT10H9M563.444333S");

            after = ((Struct) records.get(1).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(5);
            assertThat(after.getString("INTYM")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M0DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M0DT0H0M0S");

            after = ((Struct) records.get(2).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(6);
            assertThat(after.getString("INTYM")).isEqualTo("P-1Y0M0DT0H0M0S");
            assertThat(after.getString("INTYM2")).isEqualTo("P0Y-1M0DT0H0M0S");
            assertThat(after.getString("INTDS")).isEqualTo("P0Y0M-1DT0H0M0S");
            assertThat(after.getString("INTDS2")).isEqualTo("P0Y0M-7DT-5H-12M-10.0123S");

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "debezium.interval");
        }
    }

    @Test
    @FixFor("DBZ-2624")
    public void shouldSnapshotAndStreamChangesFromTableWithNumericDefaultValues() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "debezium.complex_ddl");

        try {
            // complex ddl
            final String ddl = "create table debezium.complex_ddl (" +
                    " id numeric(6) constraint customers_id_nn not null, " +
                    " name varchar2(100)," +
                    " value numeric default 1, " +
                    " constraint customers_pk primary key(id)" +
                    ")";

            // create table
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.complex_ddl to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.complex_ddl ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.complex_ddl (id, name) values (1, 'Acme')");
            connection.commit();

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.COMPLEX_DDL")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            // Perform a basic startup & initial snapshot of data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify record generated during snapshot
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.COMPLEX_DDL").size()).isEqualTo(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.complex_ddl (id, name)values (2, 'Acme2')");
            connection.commit();

            // Verify record generated during streaming
            final SourceRecords streamingRecords = consumeRecordsByTopic(1);
            assertThat(streamingRecords.recordsForTopic("server1.DEBEZIUM.COMPLEX_DDL").size()).isEqualTo(1);
        }
        finally {
            TestHelper.dropTable(connection, "debezium.complex_ddl");
        }
    }

    @Test
    @FixFor("DBZ-2683")
    @SkipOnDatabaseOption(value = "Partitioning", enabled = false)
    public void shouldSnapshotAndStreamChangesFromPartitionedTable() throws Exception {
        TestHelper.dropTable(connection, "players");
        try {
            final String ddl = "CREATE TABLE players (" +
                    "id NUMERIC(6), " +
                    "name VARCHAR(100), " +
                    "birth_date DATE," +
                    "primary key(id)) " +
                    "PARTITION BY RANGE (birth_date) (" +
                    "PARTITION p2019 VALUES LESS THAN (TO_DATE('2020-01-01', 'yyyy-mm-dd')), " +
                    "PARTITION p2020 VALUES LESS THAN (TO_DATE('2021-01-01', 'yyyy-mm-dd'))" +
                    ")";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.players to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.players ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a record to be captured by snapshot
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (1, 'Roger Rabbit', TO_DATE('2019-05-01', 'yyyy-mm-dd'))");
            connection.commit();

            // Start connector
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.PLAYERS")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.PLAYERS").size()).isEqualTo(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert a record to be captured during streaming
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (2, 'Bugs Bunny', TO_DATE('2019-06-26', 'yyyy-mm-dd'))");
            connection.execute("INSERT INTO debezium.players (id, name, birth_date) VALUES (3, 'Elmer Fud', TO_DATE('2020-11-01', 'yyyy-mm-dd'))");
            connection.commit();

            final SourceRecords streamRecords = consumeRecordsByTopic(2);
            assertThat(streamRecords.recordsForTopic("server1.DEBEZIUM.PLAYERS").size()).isEqualTo(2);
        }
        finally {
            TestHelper.dropTable(connection, "players");
        }
    }

    @Test
    @FixFor("DBZ-2849")
    public void shouldAvroSerializeColumnsWithSpecialCharacters() throws Exception {
        // Setup environment
        TestHelper.dropTable(connection, "columns_test");
        try {
            connection.execute("CREATE TABLE columns_test (id NUMERIC(6), amount$ number not null, primary key(id))");
            connection.execute("GRANT SELECT ON debezium.columns_test to " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.columns_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert a record for snapshot
            connection.execute("INSERT INTO debezium.columns_test (id, amount$) values (1, 12345.67)");
            connection.commit();

            // Start connector
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.COLUMNS_TEST")
                    .with(OracleConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, CommonConnectorConfig.SchemaNameAdjustmentMode.AVRO)
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshots = consumeRecordsByTopic(1);
            assertThat(snapshots.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").size()).isEqualTo(1);

            final SourceRecord snapshot = snapshots.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").get(0);
            VerifyRecord.isValidRead(snapshot, "ID", 1);

            Struct after = ((Struct) snapshot.value()).getStruct(AFTER);
            assertThat(after.getInt32("ID")).isEqualTo(1);
            assertThat(after.get("AMOUNT_")).isEqualTo(VariableScaleDecimal.fromLogical(after.schema().field("AMOUNT_").schema(), BigDecimal.valueOf(12345.67d)));

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert a record for streaming
            connection.execute("INSERT INTO debezium.columns_test (id, amount$) values (2, 23456.78)");
            connection.commit();

            final SourceRecords streams = consumeRecordsByTopic(1);
            assertThat(streams.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").size()).isEqualTo(1);

            final SourceRecord stream = streams.recordsForTopic("server1.DEBEZIUM.COLUMNS_TEST").get(0);
            VerifyRecord.isValidInsert(stream, "ID", 2);

            after = ((Struct) stream.value()).getStruct(AFTER);
            assertThat(after.getInt32("ID")).isEqualTo(2);
            assertThat(after.get("AMOUNT_")).isEqualTo(VariableScaleDecimal.fromLogical(after.schema().field("AMOUNT_").schema(), BigDecimal.valueOf(23456.78d)));
        }
        finally {
            TestHelper.dropTable(connection, "columns_test");
        }
    }

    @Test
    @FixFor("DBZ-2825")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void testArchiveLogScnBoundariesAreIncluded() throws Exception {
        // Drop table if it exists
        TestHelper.dropTable(connection, "alog_test");
        try {
            final String ddl = "CREATE TABLE alog_test (id numeric, name varchar2(50), primary key(id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.alog_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.alog_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            connection.commit();

            // Insert a snapshot record
            connection.execute("INSERT INTO debezium.alog_test (id, name) VALUES (1, 'Test')");
            connection.commit();

            // start connector and take snapshot
            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.ALOG_TEST")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Validate snapshot record
            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            assertThat(snapshotRecords.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").size()).isEqualTo(1);
            SourceRecord record = snapshotRecords.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").get(0);
            Struct after = (Struct) ((Struct) record.value()).get(AFTER);
            assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
            assertThat(after.get("NAME")).isEqualTo("Test");

            // stop the connector
            stopConnector();

            // Force flush of all redo logs to archive logs
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            // Start connector and wait for streaming
            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert record for streaming
            connection.execute("INSERT INTO debezium.alog_test (id, name) values (2, 'Home')");
            connection.execute("COMMIT");

            // Validate streaming record
            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").size()).isEqualTo(1);
            record = records.recordsForTopic("server1.DEBEZIUM.ALOG_TEST").get(0);
            after = (Struct) ((Struct) record.value()).get(AFTER);
            assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
            assertThat(after.get("NAME")).isEqualTo("Home");
        }
        finally {
            TestHelper.dropTable(connection, "alog_test");
        }
    }

    @Test
    @FixFor("DBZ-2784")
    public void shouldConvertDatesSpecifiedAsStringInSQL() throws Exception {
        try {
            TestHelper.dropTable(connection, "orders");

            final String ddl = "CREATE TABLE orders (" +
                    "id NUMERIC(6), " +
                    "order_date date not null," +
                    "primary key(id))";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.orders TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO debezium.orders VALUES (9, '22-FEB-2018')");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.orders")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords snapshotRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> snapshotOrders = snapshotRecords.recordsForTopic("server1.DEBEZIUM.ORDERS");
            assertThat(snapshotOrders.size()).isEqualTo(1);

            final Struct snapshotAfter = ((Struct) snapshotOrders.get(0).value()).getStruct(AFTER);
            assertThat(snapshotAfter.get("ID")).isEqualTo(9);
            assertThat(snapshotAfter.get("ORDER_DATE")).isEqualTo(1519257600000L);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.orders VALUES (10, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            final SourceRecords streamRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> orders = streamRecords.recordsForTopic("server1.DEBEZIUM.ORDERS");
            assertThat(orders).hasSize(1);

            final Struct after = ((Struct) orders.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(10);
            assertThat(after.get("ORDER_DATE")).isEqualTo(1519257600000L);
        }
        finally {
            TestHelper.dropTable(connection, "orders");
        }
    }

    @Test
    @FixFor("DBZ-2733")
    public void shouldConvertNumericAsStringDecimalHandlingMode() throws Exception {
        TestHelper.dropTable(connection, "table_number_pk");
        try {
            final String ddl = "CREATE TABLE table_number_pk (id NUMBER, name varchar2(255), age number, primary key (id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table_number_pk TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.table_number_pk ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Insert snapshot record
            connection.execute("INSERT INTO debezium.table_number_pk (id, name, age) values (1, 'Bob', 25)");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.table_number_pk")
                    .with(OracleConnectorConfig.DECIMAL_HANDLING_MODE, "string")
                    .build();

            // Start connector
            start(OracleConnector.class, config);
            assertNoRecordsToConsume();
            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Read snapshot record & verify
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK").get(0);

            List<SchemaAndValueField> expected = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.STRING_SCHEMA, "1"),
                    new SchemaAndValueField("NAME", Schema.OPTIONAL_STRING_SCHEMA, "Bob"),
                    new SchemaAndValueField("AGE", Schema.OPTIONAL_STRING_SCHEMA, "25"));
            assertRecordSchemaAndValues(expected, record, AFTER);

            // Wait for streaming to have begun
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert streaming record
            connection.execute("INSERT INTO debezium.table_number_pk (id, name, age) values (2, 'Sue', 30)");
            connection.execute("COMMIT");

            // Read stream record & verify
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK")).hasSize(1);

            record = records.recordsForTopic("server1.DEBEZIUM.TABLE_NUMBER_PK").get(0);
            expected = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.STRING_SCHEMA, "2"),
                    new SchemaAndValueField("NAME", Schema.OPTIONAL_STRING_SCHEMA, "Sue"),
                    new SchemaAndValueField("AGE", Schema.OPTIONAL_STRING_SCHEMA, "30"));
            assertRecordSchemaAndValues(expected, record, AFTER);
        }
        finally {
            TestHelper.dropTable(connection, "table_number_pk");
        }
    }

    protected void assertRecordSchemaAndValues(List<SchemaAndValueField> expectedByColumn, SourceRecord record, String envelopeFieldName) {
        Struct content = ((Struct) record.value()).getStruct(envelopeFieldName);
        if (expectedByColumn == null) {
            assertThat(content).isNull();
        }
        else {
            assertThat(content).as("expected there to be content in Envelope under " + envelopeFieldName).isNotNull();
            expectedByColumn.forEach(expected -> expected.assertFor(content));
        }
    }

    @Test
    @FixFor("DBZ-2920")
    public void shouldStreamDdlThatExceeds4000() throws Exception {
        TestHelper.dropTable(connection, "large_dml");

        // Setup table
        final String ddl = "CREATE TABLE large_dml (id NUMERIC(6), value varchar2(4000), value2 varchar2(4000), primary key(id))";
        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.large_dml TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.large_dml ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        // Prepare snapshot record
        String largeValue = generateAlphaNumericStringColumn(4000);
        String largeValue2 = generateAlphaNumericStringColumn(4000);
        connection.execute("INSERT INTO large_dml (id, value, value2) values (1, '" + largeValue + "', '" + largeValue2 + "')");
        connection.commit();

        // Start connector
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.large_dml")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();
        start(OracleConnector.class, config);
        assertNoRecordsToConsume();

        // Verify snapshot
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.topics()).hasSize(1);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.LARGE_DML")).hasSize(1);

        Struct after = ((Struct) records.recordsForTopic("server1.DEBEZIUM.LARGE_DML").get(0).value()).getStruct(AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("VALUE")).isEqualTo(largeValue);
        assertThat(after.get("VALUE2")).isEqualTo(largeValue2);

        // Prepare stream records
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        List<String> largeValues = new ArrayList<>();
        List<String> largeValues2 = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            largeValues.add(generateAlphaNumericStringColumn(4000));
            largeValues2.add(generateAlphaNumericStringColumn(4000));
            connection.execute("INSERT INTO large_dml (id, value, value2) values (" + (2 + i) + ", '" + largeValues.get(largeValues.size() - 1) + "', '"
                    + largeValues2.get(largeValues2.size() - 1) + "')");
        }
        connection.commit();

        // Verify stream
        records = consumeRecordsByTopic(10);
        assertThat(records.topics()).hasSize(1);
        assertThat(records.recordsForTopic("server1.DEBEZIUM.LARGE_DML")).hasSize(10);

        List<SourceRecord> entries = records.recordsForTopic("server1.DEBEZIUM.LARGE_DML");
        for (int i = 0; i < 10; ++i) {
            SourceRecord record = entries.get(i);
            after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(2 + i);
            assertThat(after.get("VALUE")).isEqualTo(largeValues.get(i));
            assertThat(after.get("VALUE2")).isEqualTo(largeValues2.get(i));
        }

        // Stop connector
        stopConnector((r) -> TestHelper.dropTable(connection, "large_dml"));
    }

    @Test
    @FixFor("DBZ-2891")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.XSTREAM, reason = "Only applies to Xstreams")
    public void shouldNotObserveDeadlockWhileStreamingWithXstream() throws Exception {
        long oldPollTimeInMs = pollTimeoutInMs;
        TestHelper.dropTable(connection, "deadlock_test");
        try {
            final String ddl = "CREATE TABLE deadlock_test (id numeric(9), name varchar2(50), primary key(id))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.deadlock_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.deadlock_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            // Set connector to poll very slowly
            this.pollTimeoutInMs = TimeUnit.SECONDS.toMillis(20);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "debezium.deadlock_test")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.MAX_QUEUE_SIZE, 2)
                    .with(RelationalDatabaseConnectorConfig.MAX_BATCH_SIZE, 1)
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            for (int i = 0; i < 10; ++i) {
                connection.execute("INSERT INTO deadlock_test (id, name) values (" + i + ", 'Test " + i + "')");
                connection.execute("COMMIT");
            }

            SourceRecords records = consumeRecordsByTopic(10, 24);
            assertThat(records.topics()).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DEADLOCK_TEST")).hasSize(10);

        }
        finally {
            // Reset poll time
            this.pollTimeoutInMs = oldPollTimeInMs;
            TestHelper.dropTable(connection, "deadlock_test");
        }
    }

    @Test
    @FixFor("DBZ-3057")
    public void shouldReadTableUniqueIndicesWithCharactersThatRequireExplicitQuotes() throws Exception {
        final String TABLE_NAME = "debezium.\"#T70_Sid:582003931_1_ConnConne\"";
        try {
            TestHelper.dropTable(connection, TABLE_NAME);

            final String ddl = "CREATE GLOBAL TEMPORARY TABLE " + TABLE_NAME + " (id number, name varchar2(50))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON " + TABLE_NAME + " TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE " + TABLE_NAME + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.\\#T70_Sid\\:582003931_1_ConnConne")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        finally {
            TestHelper.dropTable(connection, TABLE_NAME);
        }
    }

    @Test
    @FixFor("DBZ-3151")
    public void testSnapshotCompletesWithSystemGeneratedUniqueIndexOnKeylessTable() throws Exception {
        TestHelper.dropTable(connection, "XML_TABLE");
        try {
            final String ddl = "CREATE TABLE XML_TABLE of XMLTYPE";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON DEBEZIUM.XML_TABLE TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE DEBEZIUM.XML_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO DEBEZIUM.XML_TABLE values (xmltype('<?xml version=\"1.0\"?><tab><name>Hi</name></tab>'))");
            connection.execute("COMMIT");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.XML_TABLE")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        finally {
            TestHelper.dropTable(connection, "XML_TABLE");
        }
    }

    @Test
    @FixFor("DBZ-3001")
    public void shouldGetOracleDatabaseVersion() throws Exception {
        OracleDatabaseVersion version = connection.getOracleVersion();
        assertThat(version).isNotNull();
        assertThat(version.getMajor()).isGreaterThan(0);
    }

    @Test
    @FixFor("DBZ-3109")
    public void shouldStreamChangesForTableWithMultipleLogGroupTypes() throws Exception {
        try {
            TestHelper.dropTable(connection, "log_group_test");

            final String ddl = "CREATE TABLE log_group_test (id numeric(9,0) primary key, name varchar2(50))";
            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.log_group_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.log_group_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            connection.execute("ALTER TABLE debezium.log_group_test ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.LOG_GROUP_TEST")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.log_group_test (id, name) values (1,'Test')");
            connection.execute("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.LOG_GROUP_TEST")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "log_group_test");
        }
    }

    @Test
    @FixFor("DBZ-2875")
    public void shouldResumeStreamingAtCorrectScnOffset() throws Exception {
        TestHelper.dropTable(connection, "offset_test");
        try {
            Testing.Debug.enable();

            connection.execute("CREATE TABLE offset_test (id numeric(9,0) primary key, name varchar2(50))");
            connection.execute("GRANT SELECT ON debezium.offset_test TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.offset_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.OFFSET_TEST")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.offset_test (id, name) values (1, 'Bob')");

            SourceRecords records1 = consumeRecordsByTopic(1);
            assertThat(records1.recordsForTopic("server1.DEBEZIUM.OFFSET_TEST")).hasSize(1);

            Struct after = (Struct) ((Struct) records1.allRecordsInOrder().get(0).value()).get("after");
            Testing.print(after);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Bob");

            stopConnector();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.offset_test (id, name) values (2, 'Bill')");

            SourceRecords records2 = consumeRecordsByTopic(1);
            assertThat(records2.recordsForTopic("server1.DEBEZIUM.OFFSET_TEST")).hasSize(1);

            after = (Struct) ((Struct) records2.allRecordsInOrder().get(0).value()).get("after");
            Testing.print(after);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("NAME")).isEqualTo("Bill");
        }
        finally {
            TestHelper.dropTable(connection, "offset_test");
        }
    }

    @Test
    @FixFor("DBZ-3036")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "IOT tables are skipped")
    public void shouldHandleParentChildIndexOrganizedTables() throws Exception {
        TestHelper.dropTable(connection, "test_iot");
        try {
            String ddl = "CREATE TABLE test_iot (" +
                    "id numeric(9,0), " +
                    "description varchar2(50) not null, " +
                    "primary key(id)) " +
                    "ORGANIZATION INDEX " +
                    "INCLUDING description " +
                    "OVERFLOW";
            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.test_iot");

            // Insert data for snapshot
            connection.executeWithoutCommitting("INSERT INTO debezium.test_iot VALUES ('1', 'Hello World')");
            connection.execute("COMMIT");

            Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "(.)*IOT(.)*")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "regex")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TEST_IOT")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.TEST_IOT").get(0);
            Struct after = (Struct) ((Struct) record.value()).get(FieldName.AFTER);
            VerifyRecord.isValidRead(record, "ID", 1);
            assertThat(after.get("DESCRIPTION")).isEqualTo("Hello World");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert data for streaming
            connection.executeWithoutCommitting("INSERT INTO debezium.test_iot VALUES ('2', 'Goodbye')");
            connection.execute("COMMIT");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.TEST_IOT")).hasSize(1);

            record = records.recordsForTopic("server1.DEBEZIUM.TEST_IOT").get(0);
            after = (Struct) ((Struct) record.value()).get(FieldName.AFTER);
            VerifyRecord.isValidInsert(record, "ID", 2);
            assertThat(after.get("DESCRIPTION")).isEqualTo("Goodbye");
        }
        finally {
            TestHelper.dropTable(connection, "test_iot");
            // This makes sure all index-organized tables are cleared after dropping parent table
            TestHelper.purgeRecycleBin(connection);
        }
    }

    // todo: should this test be removed since its now covered in OracleClobDataTypesIT?
    @Test
    @FixFor("DBZ-3257")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldSnapshotAndStreamClobDataTypes() throws Exception {
        TestHelper.dropTable(connection, "clob_test");
        try {
            String ddl = "CREATE TABLE clob_test(id numeric(9,0) primary key, val_clob clob, val_nclob nclob)";
            connection.execute(ddl);
            TestHelper.streamTable(connection, "clob_test");

            connection.execute("INSERT INTO clob_test values (1, 'TestClob', 'TestNClob')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CLOB_TEST")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST")).hasSize(1);

            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST");
            VerifyRecord.isValidRead(records.get(0), "ID", 1);
            Struct after = (Struct) ((Struct) records.get(0).value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("VAL_CLOB")).isEqualTo("TestClob");
            assertThat(after.get("VAL_NCLOB")).isEqualTo("TestNClob");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("UPDATE clob_test SET val_clob = 'TestClob2', val_nclob = 'TestNClob2' WHERE ID = 1");

            sourceRecords = consumeRecordsByTopic(1);
            assertThat(sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST")).hasSize(1);

            records = sourceRecords.recordsForTopic("server1.DEBEZIUM.CLOB_TEST");
            VerifyRecord.isValidUpdate(records.get(0), "ID", 1);
            after = (Struct) ((Struct) records.get(0).value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("VAL_CLOB")).isEqualTo("TestClob2");
            assertThat(after.get("VAL_NCLOB")).isEqualTo("TestNClob2");
        }
        finally {
            TestHelper.dropTable(connection, "clob_test");
        }
    }

    @Test
    @FixFor("DBZ-3347")
    public void shouldContainPartitionInSchemaChangeEvent() throws Exception {
        TestHelper.dropTable(connection, "dbz3347");
        try {
            connection.execute("create table dbz3347 (id number primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3347");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3347")
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords schemaChanges = consumeRecordsByTopic(1);
            SourceRecord change = schemaChanges.recordsForTopic(TestHelper.SERVER_NAME).get(0);
            assertThat(change.sourcePartition()).isEqualTo(Collections.singletonMap("server", TestHelper.SERVER_NAME));
        }
        finally {
            TestHelper.dropTable(connection, "dbz3347");
        }
    }

    @Test
    @FixFor("DBZ-832")
    public void shouldSnapshotAndStreamTablesWithNoPrimaryKey() throws Exception {
        TestHelper.dropTable(connection, "dbz832");
        try {
            connection.execute("create table dbz832 (id numeric(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz832");

            connection.execute("INSERT INTO dbz832 values (1, 'Test')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ832")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ832")).hasSize(1);
            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ832").get(0);
            assertThat(record.key()).isNull();
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz832 values (2, 'Test2')");
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ832")).hasSize(1);
            record = records.recordsForTopic("server1.DEBEZIUM.DBZ832").get(0);
            assertThat(record.key()).isNull();
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Test2");

        }
        finally {
            TestHelper.dropTable(connection, "dbz832");
        }
    }

    @Test
    @FixFor("DBZ-1211")
    public void shouldSnapshotAndStreamTablesWithUniqueIndexPrimaryKey() throws Exception {
        TestHelper.dropTables(connection, "dbz1211_child", "dbz1211");
        try {
            connection.execute("create table dbz1211 (id numeric(9,0), data varchar2(50), constraint pkdbz1211 primary key (id) using index)");
            connection.execute("alter table dbz1211 add constraint xdbz1211 unique (id,data) using index");
            connection
                    .execute("create table dbz1211_child (id numeric(9,0), data varchar2(50), constraint fk1211 foreign key (id) references dbz1211 on delete cascade)");
            connection.execute("alter table dbz1211_child add constraint ydbz1211 unique (id,data) using index");
            TestHelper.streamTable(connection, "dbz1211");
            TestHelper.streamTable(connection, "dbz1211_child");

            connection.executeWithoutCommitting("INSERT INTO dbz1211 values (1, 'Test')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (1, 'Child')");
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1211,DEBEZIUM\\.DBZ1211\\_CHILD")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211")).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211").get(0);
            Struct key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.schema().field("DATA")).isNull();
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.get("DATA")).isEqualTo("Child");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Child");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz1211 values (2, 'Test2')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (1, 'Child1-2')");
            connection.executeWithoutCommitting("INSERT INTO dbz1211_child values (2, 'Child2-1')");
            connection.commit();

            records = consumeRecordsByTopic(3);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211")).hasSize(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD")).hasSize(2);

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(2);
            assertThat(key.schema().field("DATA")).isNull();
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Test2");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(0);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(1);
            assertThat(key.get("DATA")).isEqualTo("Child1-2");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Child1-2");

            record = records.recordsForTopic("server1.DEBEZIUM.DBZ1211_CHILD").get(1);
            key = (Struct) record.key();
            assertThat(key).isNotNull();
            assertThat(key.get("ID")).isEqualTo(2);
            assertThat(key.get("DATA")).isEqualTo("Child2-1");
            after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("DATA")).isEqualTo("Child2-1");
        }
        finally {
            TestHelper.dropTables(connection, "dbz1211_child", "dbz1211");
        }
    }

    @Test
    @FixFor("DBZ-3322")
    public void shouldNotEmitEventsOnConstraintViolations() throws Exception {
        TestHelper.dropTable(connection, "dbz3322");
        try {
            connection.execute("CREATE TABLE dbz3322 (id number(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz3322 ON dbz3322 (id)");
            TestHelper.streamTable(connection, "dbz3322");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3322")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try {
                connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test1')");
                connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test2')");
            }
            catch (SQLException e) {
                // ignore unique constraint violation
                if (!e.getMessage().startsWith("ORA-00001")) {
                    throw e;
                }
            }
            finally {
                connection.executeWithoutCommitting("COMMIT");
            }

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3322")).hasSize(1);

            final Struct after = (((Struct) records.recordsForTopic("server1.DEBEZIUM.DBZ3322").get(0).value()).getStruct("after"));
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test1");

            assertNoRecordsToConsume();

        }
        finally {
            TestHelper.dropTable(connection, "dbz3322");
        }
    }

    @Test
    @FixFor("DBZ-5090")
    public void shouldNotEmitEventsOnConstraintViolationsAcrossSessions() throws Exception {
        TestHelper.dropTable(connection, "dbz5090");
        try {
            connection.execute("CREATE TABLE dbz5090 (id number(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz5090 ON dbz5090 (id)");
            TestHelper.streamTable(connection, "dbz5090");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5090")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // We require the use of an executor here so that the multiple threads cooperate with one
            // another in a way that does not block the test moving forward in the various stages.
            ExecutorService executorService = Executors.newFixedThreadPool(2);

            try (OracleConnection connection2 = TestHelper.testConnection(); OracleConnection connection3 = TestHelper.testConnection()) {

                connection.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (1,'Test1')");

                // Task that creates in-progress transaction with second connection
                final CountDownLatch latchA = new CountDownLatch(2);
                final CountDownLatch latchB = new CountDownLatch(1);
                final List<Future<Boolean>> futures = new ArrayList<>();

                // Task that creates in-progress transaction with second connection
                futures.add(executorService.submit(() -> {
                    try {
                        connection2.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (2,'Test2')");

                        latchA.countDown();
                        try {
                            connection2.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (1,'Test2')");
                        }
                        catch (SQLException e) {
                            // Test that transaction state isn't tainted if user retries multiple times per session
                            // and gets repeated SQL exceptions such as constraint violations for duplicate PKs.
                            latchB.await();
                            connection2.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (1,'Test2')");
                        }
                        return true;
                    }
                    catch (SQLException e) {
                        return false;
                    }
                }));

                // Task that creates in-progress transaction with third connection
                futures.add(executorService.submit(() -> {
                    try {
                        connection3.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (3,'Test3')");

                        latchA.countDown();
                        try {
                            connection3.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (1,'Test3')");
                        }
                        catch (SQLException e) {
                            // Test that transaction state isn't tainted if user retries multiple times per session
                            // and gets repeated SQL exceptions such as constraint violations for duplicate PKs.
                            latchB.await();
                            connection3.executeWithoutCommitting("INSERT INTO dbz5090 (id,data) values (1,'Test3b')");
                        }
                        return true;
                    }
                    catch (SQLException e) {
                        return false;
                    }
                }));

                // We wait until the latch has been triggered by the callable task
                latchA.await();

                // Explicitly wait 5 seconds to guarantee that the thread has executed the SQL
                Thread.sleep(5000);

                connection.commit();

                // toggle each thread's second attempt
                latchB.countDown();

                // Get thread state, should return false due to constraint violation
                assertThat(futures.get(0).get()).isFalse();
                assertThat(futures.get(1).get()).isFalse();

                // Each connection inserts one new row and attempts a duplicate insert of an existing PK
                // so the connection needs to be committed to guarantee that we test the scenario where
                // we get a transaction commit & need to filter out roll-back rows rather than the
                // transaction being rolled back entirely.
                connection2.commit();
                connection3.commit();
            }

            final SourceRecords sourceRecords = consumeRecordsByTopic(3);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5090");
            assertThat(records).hasSize(3);

            VerifyRecord.isValidInsert(records.get(0), "ID", 1);

            final Struct after = (((Struct) records.get(0).value()).getStruct("after"));
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test1");

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5090");
        }
    }

    @Test
    @FixFor("DBZ-3322")
    public void shouldNotEmitEventsInRollbackTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz3322");
        try {
            connection.execute("CREATE TABLE dbz3322 (id number(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX uk_dbz3322 ON dbz3322 (id)");
            TestHelper.streamTable(connection, "dbz3322");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3322")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (1, 'Test')");
            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (2, 'Test')");
            connection.executeWithoutCommitting("ROLLBACK");

            connection.executeWithoutCommitting("INSERT INTO dbz3322 (id,data) values (3, 'Test')");
            connection.executeWithoutCommitting("COMMIT");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3322")).hasSize(1);
            Struct value = (Struct) records.recordsForTopic("server1.DEBEZIUM.DBZ3322").get(0).value();
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(3);
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3322");
        }
    }

    @Test
    @FixFor("DBZ-3062")
    public void shouldSelectivelySnapshotTables() throws Exception {
        TestHelper.dropTables(connection, "dbz3062a", "dbz3062b");
        try {
            connection.execute("CREATE TABLE dbz3062a (id number(9,0), data varchar2(50))");
            connection.execute("CREATE TABLE dbz3062b (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3062a");
            TestHelper.streamTable(connection, "dbz3062b");

            connection.execute("INSERT INTO dbz3062a VALUES (1, 'Test1')");
            connection.execute("INSERT INTO dbz3062b VALUES (2, 'Test2')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3062.*")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE_TABLES, "[A-z].*DEBEZIUM\\.DBZ3062A")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "regex")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableA = records.recordsForTopic("server1.DEBEZIUM.DBZ3062A");
            List<SourceRecord> tableB = records.recordsForTopic("server1.DEBEZIUM.DBZ3062B");

            assertThat(tableA).hasSize(1);
            assertThat(tableB).isNull();

            Struct after = ((Struct) tableA.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("Test1");

            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz3062a VALUES (3, 'Test3')");
            connection.executeWithoutCommitting("INSERT INTO dbz3062b VALUES (4, 'Test4')");
            connection.commit();

            records = consumeRecordsByTopic(2);
            tableA = records.recordsForTopic("server1.DEBEZIUM.DBZ3062A");
            tableB = records.recordsForTopic("server1.DEBEZIUM.DBZ3062B");

            assertThat(tableA).hasSize(1);
            assertThat(tableB).hasSize(1);

            after = ((Struct) tableA.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("DATA")).isEqualTo("Test3");

            after = ((Struct) tableB.get(0).value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(4);
            assertThat(after.get("DATA")).isEqualTo("Test4");
        }
        finally {
            TestHelper.dropTables(connection, "dbz3062a", "dbz3062b");
        }
    }

    @Test
    @FixFor("DBZ-3616")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldNotLogWarningsAboutCommittedTransactionsWhileStreamingNormally() throws Exception {
        TestHelper.dropTables(connection, "dbz3616", "dbz3616");
        try {
            connection.execute("CREATE TABLE dbz3616 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3616");
            connection.commit();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3616.*")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "regex")
                    // use online_catalog mode explicitly due to Awaitility timer below.
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            // Start connector
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // Wait for streaming
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start second connection and write an insert, without commit.
            OracleConnection connection2 = TestHelper.testConnection();
            connection2.executeWithoutCommitting("INSERT INTO dbz3616 (id,data) values (1,'Conn2')");

            // One first connection write an insert without commit, then explicitly commit it.
            connection.executeWithoutCommitting("INSERT INTO dbz3616 (id,data) values (2,'Conn1')");
            connection.commit();

            // Connector should continually re-mine the first transaction until committed.
            // During this time, there should not be these extra log messages that we
            // will assert against later via LogInterceptor.
            Awaitility.await()
                    .pollDelay(Durations.ONE_MINUTE)
                    .timeout(Durations.TWO_MINUTES)
                    .until(() -> true);

            // Now commit connection2, this means we should get 2 inserts.
            connection2.commit();

            // Now get the 2 records, two inserts from both transactions
            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3616")).hasSize(2);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3616");
            assertThat(((Struct) tableRecords.get(0).value()).getStruct("after").get("ID")).isEqualTo(2);
            assertThat(((Struct) tableRecords.get(1).value()).getStruct("after").get("ID")).isEqualTo(1);
        }
        finally {
            TestHelper.dropTables(connection, "dbz3616", "dbz3616");
        }
    }

    @Test
    @FixFor("DBZ-3668")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        connection.execute("INSERT INTO customer (id,name,score) values (1001, 'DBZ3668', 100)");

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> customers = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(customers).hasSize(1);

        for (SourceRecord customer : customers) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(customer, "oracle", "server1", false);
        }

        connection.execute("INSERT INTO customer (id,name,score) values (1002, 'DBZ3668', 95)");
        records = consumeRecordsByTopic(1);

        customers = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(customers).hasSize(1);

        for (SourceRecord customer : customers) {
            CloudEventsConverterTest.shouldConvertToCloudEventsInJson(customer, false, jsonNode -> {
                assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("scn:");
            });
            CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(customer, false);
            CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(customer, "oracle", "server1", false);
        }
    }

    @Test
    @FixFor("DBZ-3896")
    public void shouldCaptureTableMetadataWithMultipleStatements() throws Exception {
        try {
            Configuration config = TestHelper.defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3896").build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("CREATE TABLE dbz3896 (id number(9,0), name varchar2(50), data varchar2(50))",
                    "CREATE UNIQUE INDEX dbz3896_pk ON dbz3896 (\"ID\", \"NAME\")",
                    "ALTER TABLE dbz3896 ADD CONSTRAINT idx_dbz3896 PRIMARY KEY (\"ID\", \"NAME\") USING INDEX \"DBZ3896_PK\"");
            TestHelper.streamTable(connection, "dbz3896");
            connection.execute("INSERT INTO dbz3896 (id,name,data) values (1,'First','Test')");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3896")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ3896").get(0);
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema().field("ID")).isNotNull();
            assertThat(record.keySchema().field("NAME")).isNotNull();
            assertThat(((Struct) record.key()).get("ID")).isEqualTo(1);
            assertThat(((Struct) record.key()).get("NAME")).isEqualTo("First");
        }
        finally {
            TestHelper.dropTable(connection, "dbz3896");
        }
    }

    @Test
    @FixFor("DBZ-3898")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Tests specific LogMiner features")
    @SkipOnDatabaseOption(value = "Real Application Clusters", enabled = true, reason = "Performance w/CATALOG_IN_REDO on Oracle RAC")
    public void shouldIgnoreAllTablesInExcludedSchemas() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz3898");

            connection.execute("CREATE TABLE dbz3898 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3898");

            // Explicitly uses CATALOG_IN_REDO mining strategy
            // This strategy makes changes to several LOGMNR tables as DDL tracking gets enabled
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, LogMiningStrategy.CATALOG_IN_REDO)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz3898 (id,data) values (1,'Test')");

            final Scn scnAfterInsert = TestHelper.getCurrentScn();

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3898")).hasSize(1);

            // Wait for the connector to advance beyond the current SCN after the INSERT.
            Awaitility.await().atMost(Duration.ofMinutes(3))
                    .until(() -> new Scn(getStreamingMetric("CurrentScn")).compareTo(scnAfterInsert) > 0);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3898");
        }
    }

    @Test
    @FixFor({ "DBZ-3712", "DBZ-4879" })
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void shouldStartWithArchiveLogOnlyModeAndStreamWhenRecordsBecomeAvailable() throws Exception {
        TestHelper.dropTable(connection, "dbz3712");
        try {
            connection.execute("CREATE TABLE dbz3712 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3712");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // At this point the connector is new and should not emit any records as the SCN offset
            // obtained from the snapshot is in the redo logs.
            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            assertNoRecordsToConsume();

            // We will insert a new record but this record won't be emitted right away and will
            // require that a log switch happen so that it can be emitted.
            connection.execute("INSERT INTO dbz3712 (id,data) values (1, 'Test')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);

            // Now insert a new record but this record won't be emitted because it will require
            // a log switch to happen so it can be emitted.
            connection.execute("INSERT INTO dbz3712 (id,data) values (2, 'Test2')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3712");
        }
    }

    @Test
    @FixFor("DBZ-3712")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Tests archive log support for LogMiner only")
    public void shouldPermitChangingToArchiveLogOnlyModeOnExistingConnector() throws Exception {
        TestHelper.dropTable(connection, "dbz3712");
        try {
            connection.execute("CREATE TABLE dbz3712 (id number(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz3712");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // The connector was started with archive.log.only.mode disabled so this record should
            // be emitted immediately once its written to the redo logs.
            connection.execute("INSERT INTO dbz3712 (id,data) values (1, 'Test1')");

            // We should now be able to consume a record
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);

            // Restart connector using the same offsets but with archive log only mode
            stopConnector();

            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                    .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_SCN_POLL_INTERVAL_MS, 2000)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3712")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // At this point the connector was restarted with archive log only mode. The SCN offset
            // was previously in the redo logs and may likely not be in the archive logs on start so
            // we'll give the connector a moment and verify it has no records to consume.
            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            assertNoRecordsToConsume();

            // Insert a new record
            // This should not be picked up until after a log switch
            connection.execute("INSERT INTO dbz3712 (id,data) values (2, 'Test2')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);

            // Insert a new record
            // This should not be picked up until after a log switch
            connection.execute("INSERT INTO dbz3712 (id,data) values (3, 'Test2')");
            waitForLogSwitchOrForceOneAfterTimeout();

            // We should now be able to consume a record
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3712")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz3712");
        }
    }

    private void waitForLogSwitchOrForceOneAfterTimeout() throws SQLException {
        List<BigInteger> sequences = TestHelper.getCurrentRedoLogSequences();
        try {
            Awaitility.await()
                    .pollInterval(Duration.of(5, ChronoUnit.SECONDS))
                    .atMost(Duration.of(20, ChronoUnit.SECONDS))
                    .until(() -> {
                        if (TestHelper.getCurrentRedoLogSequences().equals(sequences)) {
                            assertNoRecordsToConsume();
                            return false;
                        }
                        // Oracle triggered its on log switch
                        return true;
                    });

            // In this use case Oracle triggered its own log switch
            // We don't need to trigger one on our own.
        }
        catch (ConditionTimeoutException e) {
            // expected if Oracle doesn't trigger its own log switch
            TestHelper.forceLogfileSwitch();
        }
    }

    @Test
    @FixFor("DBZ-5756")
    public void testShouldIgnoreCompressionAdvisorTablesDuringSnapshotAndStreaming() throws Exception {
        // This test creates a dummy table to mimic the creation of a compression advisor table.
        TestHelper.dropTable(connection, "CMP3$12345");
        try {

            // Create the advisor table prior to the connector starting
            connection.execute("CREATE TABLE CMP3$12345 (id numeric(9,0), id2 numeric(9,0), data varchar2(50), primary key(id, id2))");
            TestHelper.streamTable(connection, "CMP3$12345");

            // insert some data
            connection.execute("INSERT INTO CMP3$12345 (id,id2,data) values (1, 1, 'data')");

            Configuration config = TestHelper.defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CMP.*").build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // insert some data
            connection.execute("INSERT INTO CMP3$12345 (id,id2,data) values (2, 2, 'data')");

            try {
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
                    assertNoRecordsToConsume();
                    return false;
                });
            }
            catch (ConditionTimeoutException e) {
                // expected
            }
        }
        finally {
            TestHelper.dropTable(connection, "CMP3$12345");
        }
    }

    @Test
    @FixFor("DBZ-5756")
    public void testShouldIgnoreCompressionAdvisorTablesDuringStreaming() throws Exception {
        // This test creates a dummy table to mimic the creation of a compression advisor table.
        TestHelper.dropTable(connection, "CMP3$12345");
        try {
            Configuration config = TestHelper.defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CMP.*").build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Create the advisor table while the connector is running
            connection.execute("CREATE TABLE CMP3$12345 (id numeric(9,0), id2 numeric(9,0), data varchar2(50), primary key(id, id2))");
            TestHelper.streamTable(connection, "CMP3$12345");

            // insert some data
            connection.execute("INSERT INTO CMP3$12345 (id,id2,data) values (1, 1, 'data')");

            try {
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
                    assertNoRecordsToConsume();
                    return false;
                });
            }
            catch (ConditionTimeoutException e) {
                // expected
            }
        }
        finally {
            TestHelper.dropTable(connection, "CMP3$12345");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getStreamingMetric(String metricName) throws JMException {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        final ObjectName objectName = getStreamingMetricsObjectName(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        return (T) mbeanServer.getAttribute(objectName, metricName);
    }

    private String generateAlphaNumericStringColumn(int size) {
        final String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        final StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; ++i) {
            int index = (int) (alphaNumericString.length() * Math.random());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    private void verifyHeartbeatRecord(SourceRecord heartbeat) {
        assertEquals("__debezium-heartbeat.server1", heartbeat.topic());

        Struct key = (Struct) heartbeat.key();
        assertThat(key.get("serverName")).isEqualTo("server1");
    }

    private long toMicroSecondsSinceEpoch(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND;
    }

    @Test(expected = DebeziumException.class)
    @FixFor("DBZ-3986")
    public void shouldCreateSnapshotSchemaOnlyRecoveryExceptionWithoutOffset() {
        final Path path = Testing.Files.createTestingPath("missing-history.txt").toAbsolutePath();
        Configuration config = defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY)
                .with(FileSchemaHistory.FILE_PATH, path)
                .build();

        // Start the connector ...
        AtomicReference<Throwable> exception = new AtomicReference<>();
        start(OracleConnector.class, config, (success, message, error) -> exception.set(error));
        Testing.Files.delete(path);
        throw (RuntimeException) exception.get();
    }

    @Test
    @FixFor("DBZ-3986")
    public void shouldCreateSnapshotSchemaOnlyRecovery() throws Exception {
        try {
            Configuration.Builder builder = defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3986")
                    .with(OracleConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName())
                    .with(EmbeddedEngineConfig.OFFSET_STORAGE, FileOffsetBackingStore.class.getName());
            Configuration config = builder.build();
            consumeRecords(config);

            // Insert a row of data in advance
            connection.execute("INSERT INTO DBZ3986 (ID, DATA) values (3, 'asuka')");
            builder.with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY);
            config = builder.build();

            start(OracleConnector.class, config);

            int recordCount = 1;
            SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);

            // Compare data
            assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
            Struct struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(3, struct.get("ID"));
            assertEquals("asuka", struct.get("DATA"));
        }
        finally {
            TestHelper.dropTable(connection, "DBZ3986");
        }
    }

    @Test(expected = DebeziumException.class)
    @FixFor("DBZ-3986")
    public void shouldCreateSnapshotSchemaOnlyExceptionWithoutHistory() throws Exception {
        try {
            Configuration.Builder builder = defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3986")
                    .with(OracleConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName())
                    .with(EmbeddedEngineConfig.OFFSET_STORAGE, FileOffsetBackingStore.class.getName());
            Configuration config = builder.build();
            consumeRecords(config);

            AtomicReference<Throwable> exception = new AtomicReference<>();
            start(OracleConnector.class, config, (success, message, error) -> exception.set(error));
            throw (RuntimeException) exception.get();
        }
        finally {
            TestHelper.dropTable(connection, "DBZ3986");
        }
    }

    @Test
    @FixFor("DBZ-3986")
    public void shouldSkipDataOnSnapshotSchemaOnly() throws Exception {
        try {
            Configuration.Builder builder = defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3986")
                    .with(OracleConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class.getName())
                    .with(EmbeddedEngineConfig.OFFSET_STORAGE, MemoryOffsetBackingStore.class.getName());
            Configuration config = builder.build();
            consumeRecords(config);

            // Insert a row of data in advance. And should skip the data
            connection.execute("INSERT INTO DBZ3986 (ID, DATA) values (3, 'asuka')");

            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO DBZ3986 (ID, DATA) values (4, 'debezium')");
            int recordCount = 1;
            SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);

            // Compare data
            assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
            Struct struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(4, struct.get("ID"));
            assertEquals("debezium", struct.get("DATA"));
        }
        finally {
            TestHelper.dropTable(connection, "DBZ3986");
        }
    }

    @Test
    @FixFor("DBZ-4161")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies to LogMiner only")
    public void shouldWarnAboutTableNameLengthExceeded() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");

            connection.execute("CREATE TABLE dbz4161_with_a_name_that_is_greater_than_30 (id numeric(9,0), data varchar2(30))");
            TestHelper.streamTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");

            connection.execute("INSERT INTO dbz4161_with_a_name_that_is_greater_than_30 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30")
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4161_with_a_name_that_is_greater_than_30 values (2, 'streaming')");
            waitForCurrentScnToHaveBeenSeenByConnector();

            assertNoRecordsToConsume();
            assertThat(logInterceptor.containsWarnMessage("Table 'DBZ4161_WITH_A_NAME_THAT_IS_GREATER_THAN_30' won't be captured by Oracle LogMiner")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4161_with_a_name_that_is_greater_than_30");
        }
    }

    @Test
    @FixFor("DBZ-4161")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies to LogMiner only")
    public void shouldWarnAboutColumnNameLengthExceeded() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4161");

            connection.execute("CREATE TABLE dbz4161 (id numeric(9,0), a_very_long_column_name_that_is_greater_than_30 varchar2(30))");
            TestHelper.streamTable(connection, "dbz4161");

            connection.execute("INSERT INTO dbz4161 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4161")
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4161")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ4161").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("A_VERY_LONG_COLUMN_NAME_THAT_IS_GREATER_THAN_30")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4161 values (2, 'streaming')");
            waitForCurrentScnToHaveBeenSeenByConnector();

            assertNoRecordsToConsume();
            assertThat(logInterceptor.containsWarnMessage("Table 'DBZ4161' won't be captured by Oracle LogMiner")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4161");
        }
    }

    @Test
    @FixFor("DBZ-3611")
    public void shouldSafelySnapshotAndStreamWithDatabaseIncludeList() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz3611");

            connection.execute("CREATE TABLE dbz3611 (id numeric(9,0), data varchar2(30))");
            TestHelper.streamTable(connection, "dbz3611");

            connection.execute("INSERT INTO dbz3611 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.DATABASE_INCLUDE_LIST, TestHelper.getDatabaseName())
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.dbz3611")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3611")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ3611").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz3611 values (2, 'streaming')");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3611")).hasSize(1);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3611");
        }
    }

    /**
     * database include/exclude list are not support (yet) for the Oracle connector; this test is just there to make
     * sure that the presence of these (functionally ignored) properties doesn't cause any problems.
     */
    @Test
    @FixFor("DBZ-3611")
    public void shouldSafelySnapshotAndStreamWithDatabaseExcludeList() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz3611");

            connection.execute("CREATE TABLE dbz3611 (id numeric(9,0), data varchar2(30))");
            TestHelper.streamTable(connection, "dbz3611");

            connection.execute("INSERT INTO dbz3611 values (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.DATABASE_EXCLUDE_LIST, TestHelper.getDatabaseName() + "2")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.dbz3611")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3611")).hasSize(1);

            SourceRecord record = records.recordsForTopic("server1.DEBEZIUM.DBZ3611").get(0);
            Struct after = ((Struct) record.value()).getStruct(AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("snapshot");

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz3611 values (2, 'streaming')");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ3611")).hasSize(1);

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz3611");
        }
    }

    @Test
    @FixFor("DBZ-4376")
    public void shouldNotRaiseNullPointerExceptionWithNonUppercaseDatabaseName() throws Exception {
        // the snapshot process would throw a NPE due to a lowercase PDB or DBNAME setup
        final Configuration config;
        if (TestHelper.isUsingPdb()) {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.PDB_NAME, TestHelper.getDatabaseName().toLowerCase())
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.DATABASE_NAME, TestHelper.getDatabaseName().toLowerCase())
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                    .build();
        }

        connection.execute("INSERT INTO debezium.customer (id,name) values (1, 'Bugs Bunny')");
        start(OracleConnector.class, config);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(((Struct) records.get(0).value()).getStruct(AFTER).get("ID")).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-8710")
    public void shouldTreatQuotedPdbOrDatabaseNameAsCaseSensitive() throws Exception {
        final Configuration config;
        if (TestHelper.isUsingPdb()) {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.PDB_NAME, "\"" + TestHelper.getDatabaseName() + "\"")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                    .build();
        }
        else {
            config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.DATABASE_NAME, "\"" + TestHelper.getDatabaseName() + "\"")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                    .build();
        }

        connection.execute("INSERT INTO debezium.customer (id,name) values (1, 'Bugs Bunny')");
        start(OracleConnector.class, config);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(((Struct) records.get(0).value()).getStruct(AFTER).get("ID")).isEqualTo(1);
    }

    @FixFor("DBZ-3986")
    private void consumeRecords(Configuration config) throws SQLException, InterruptedException {
        // Poll for records ...
        TestHelper.dropTable(connection, "DBZ3986");
        connection.execute("CREATE TABLE DBZ3986 (ID number(9,0), DATA varchar2(50))");
        TestHelper.streamTable(connection, "DBZ3986");

        // Start the connector ...
        start(OracleConnector.class, config);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        connection.execute("INSERT INTO DBZ3986 (ID, DATA) values (1, 'Test')");
        connection.execute("INSERT INTO DBZ3986 (ID, DATA) values (2, 'ashlin')");
        int recordCount = 2;
        SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
        stopConnector();
    }

    @Test
    @FixFor("DBZ-4367")
    public void shouldCaptureChangesForTransactionsAcrossSnapshotBoundary() throws Exception {
        TestHelper.dropTable(connection, "DBZ4367");
        try {
            connection.execute("CREATE TABLE DBZ4367 (ID number(9, 0), DATA varchar2(50))");
            TestHelper.streamTable(connection, "DBZ4367");

            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (1, 'pre-snapshot pre TX')");
            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (2, 'pre-snapshot in TX')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4367")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY)
                    .build();
            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (3, 'post-snapshot in TX')");
            connection.executeWithoutCommitting("COMMIT");
            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (4, 'post snapshot post TX')");

            SourceRecords records = consumeRecordsByTopic(4);
            List<Integer> ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(1, 2, 3, 4);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(4);
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, "DBZ4367");
        }
    }

    @Test
    @FixFor("DBZ-4367")
    public void shouldCaptureChangesForTransactionsAcrossSnapshotBoundaryWithoutDuplicatingSnapshottedChanges() throws Exception {
        OracleConnection secondConnection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "DBZ4367");
        try {
            connection.execute("CREATE TABLE DBZ4367 (ID number(9, 0), DATA varchar2(50))");
            TestHelper.streamTable(connection, "DBZ4367");

            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (1, 'pre-snapshot pre TX')");
            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (2, 'pre-snapshot in TX')");
            secondConnection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (3, 'pre-snapshot in another TX')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4367")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY)
                    .build();
            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(2);
            List<Integer> ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(1, 3);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(2);

            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (4, 'post-snapshot in TX')");
            connection.executeWithoutCommitting("COMMIT");
            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (5, 'post snapshot post TX')");

            records = consumeRecordsByTopic(3);
            ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(2, 4, 5);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(3);
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, "DBZ4367");
            secondConnection.close();
        }
    }

    @Test
    @FixFor("DBZ-4367")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "User-defined types not supported")
    public void shouldCaptureChangesForTransactionsAcrossSnapshotBoundaryWithoutReemittingDDLChanges() throws Exception {
        OracleConnection secondConnection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "DBZ4367");
        TestHelper.dropTable(connection, "DBZ4367_EXTRA");
        try {
            connection.execute("CREATE TABLE DBZ4367 (ID number(9, 0), DATA varchar2(50))");
            TestHelper.streamTable(connection, "DBZ4367");
            connection.execute("CREATE TABLE DBZ4367_EXTRA (ID number(9, 0), DATA varchar2(50))");
            TestHelper.streamTable(connection, "DBZ4367_EXTRA");

            // some transactions that are complete pre-snapshot
            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (1, 'pre-snapshot pre TX')");
            connection.execute("INSERT INTO DBZ4367_EXTRA (ID, DATA) VALUES (100, 'second table, pre-snapshot pre TX')");

            // this is a transaction that spans the snapshot boundary, changes should be streamed
            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (2, 'pre-snapshot in TX')");
            // pre-snapshot DDL, should not be emitted in the streaming phase
            secondConnection.execute("ALTER TABLE DBZ4367_EXTRA ADD DATA2 VARCHAR2(50) DEFAULT 'default2'");
            secondConnection.execute("ALTER TABLE DBZ4367_EXTRA ADD DATA3 VARCHAR2(50) DEFAULT 'default3'");
            secondConnection.execute("INSERT INTO DBZ4367_EXTRA (ID, DATA, DATA2, DATA3) VALUES (150, 'second table, with outdated schema', 'something', 'something')");
            secondConnection.execute("ALTER TABLE DBZ4367_EXTRA DROP COLUMN DATA3");
            connection.executeWithoutCommitting("INSERT INTO DBZ4367_EXTRA (ID, DATA, DATA2) VALUES (200, 'second table, pre-snapshot in TX', 'something')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4367,DEBEZIUM\\.DBZ4367_EXTRA")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.TRANSACTION_VIEW_ONLY)
                    .build();
            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records;
            List<SourceRecord> ddls;
            List<Integer> ids;

            // we expect six DDL records (synthetic CREATEs for the final table structure) and three DML records (one for each insert)
            records = consumeRecordsByTopic(9);
            ddls = records.ddlRecordsForDatabase(TestHelper.getDatabaseName());
            ddls.forEach(r -> assertThat(((Struct) r.value()).getString("ddl")).contains("CREATE TABLE"));
            assertThat(ddls).hasSize(6);
            ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsExactly(1);
            ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367_EXTRA").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(100, 150);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(2);

            connection.executeWithoutCommitting("INSERT INTO DBZ4367 (ID, DATA) VALUES (3, 'post-snapshot in TX')");
            connection.executeWithoutCommitting("INSERT INTO DBZ4367_EXTRA (ID, DATA, DATA2) VALUES (300, 'second table, post-snapshot in TX', 'something')");
            connection.executeWithoutCommitting("COMMIT");
            // snapshot-spanning transaction ends here

            // post-snapshot transaction, changes should be streamed
            connection.execute("INSERT INTO DBZ4367 (ID, DATA) VALUES (4, 'post snapshot post TX')");
            connection.execute("INSERT INTO DBZ4367_EXTRA (ID, DATA, DATA2) VALUES (400, 'second table, post-snapshot post TX', 'something')");

            records = consumeRecordsByTopic(6);
            ddls = records.ddlRecordsForDatabase(TestHelper.getDatabaseName());
            if (ddls != null) {
                assertThat(ddls).isEmpty();
            }
            ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(2, 3, 4);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(3);
            ids = records.recordsForTopic("server1.DEBEZIUM.DBZ4367_EXTRA").stream()
                    .map(r -> getAfter(r).getInt32("ID"))
                    .collect(Collectors.toList());
            assertThat(ids).containsOnly(200, 300, 400);
            assertThat(ids).doesNotHaveDuplicates();
            assertThat(ids).hasSize(3);
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, "DBZ4367");
            TestHelper.dropTable(connection, "DBZ4367_EXTRA");
            secondConnection.close();
        }
    }

    @Test
    @FixFor("DBZ-5085")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to LogMiner")
    public void shouldSnapshotAndStreamAllRecordsThatSpanAcrossSnapshotStreamingBoundarySmallTrxs() throws Exception {
        TestHelper.dropTable(connection, "dbz5085");
        try {
            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingAdapter.class);

            setConsumeTimeout(10, TimeUnit.SECONDS);

            connection.execute("CREATE TABLE dbz5085 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5085");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5085")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.ALL)
                    .build();

            final int expected = 50;

            // Insert records into the table while the connector starts, part of the records should be
            // captured during snapshot and streaming. We just need to guarantee that we get all records.
            LOGGER.info("Inserting {} records", expected);
            for (int i = 0; i < expected; ++i) {
                if (i % 2 == 0) {
                    connection.execute("INSERT INTO dbz5085 (id,data) values (" + i + ", 'Test-" + i + "')");
                }
                else {
                    connection.executeWithoutCommitting("INSERT INTO dbz5085 (id,data) values (" + i + ", 'Test-" + i + "')");
                }
                // simulate longer lived transactions
                Thread.sleep(100L);
            }

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // make sure transaction doesn't commit too early
            Awaitility.await().atMost(Duration.ofMinutes(3)).until(() -> logInterceptor.containsMessage("Pending Transaction '"));

            connection.commit();

            // wait until we get to streaming phase
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(expected);

            // verify that we got expected numbers of records
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5085");
            assertThat(records).hasSize(expected);

            boolean snapshotFound = false;
            boolean streamingFound = false;
            for (int i = 0; i < expected; ++i) {
                final SourceRecord record = records.get(i);
                final Struct value = (Struct) record.value();
                if (value.getString("op").equals(Envelope.Operation.READ.code())) {
                    snapshotFound = true;
                    VerifyRecord.isValidRead(record, "ID", i);
                }
                else {
                    streamingFound = true;
                    VerifyRecord.isValidInsert(record, "ID", i);
                }
            }

            // Verify that we got records from both phases
            assertThat(snapshotFound).isTrue();
            assertThat(streamingFound).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5085");
        }
    }

    @Test
    @FixFor("DBZ-5085")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to LogMiner")
    public void shouldSnapshotAndStreamAllRecordsThatSpanAcrossSnapshotStreamingBoundaryLargeTrxs() throws Exception {
        TestHelper.dropTable(connection, "dbz5085");
        try {
            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingAdapter.class);

            setConsumeTimeout(10, TimeUnit.SECONDS);

            connection.execute("CREATE TABLE dbz5085 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5085");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5085")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_SNAPSHOT_BOUNDARY_MODE, TransactionSnapshotBoundaryMode.ALL)
                    .build();

            final int expected = 50;

            // Insert records into the table while the connector starts, part of the records should be
            // captured during snapshot and streaming. We just need to guarantee that we get all records.
            LOGGER.info("Inserting {} records", expected);
            for (int i = 0; i < expected; ++i) {
                if (i % 10 == 0) {
                    connection.execute("INSERT INTO dbz5085 (id,data) values (" + i + ", 'Test-" + i + "')");
                }
                else {
                    connection.executeWithoutCommitting("INSERT INTO dbz5085 (id,data) values (" + i + ", 'Test-" + i + "')");
                }
                // simulate longer lived transactions
                Thread.sleep(100L);
            }

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // make sure transaction doesn't commit too early
            Awaitility.await().atMost(Duration.ofMinutes(3)).until(() -> logInterceptor.containsMessage("Pending Transaction '"));

            connection.commit();

            // wait until we get to streaming phase
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(expected);

            // verify that we got expected numbers of records
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5085");
            assertThat(records).hasSize(expected);

            boolean snapshotFound = false;
            boolean streamingFound = false;
            for (int i = 0; i < expected; ++i) {
                final SourceRecord record = records.get(i);
                final Struct value = (Struct) record.value();
                if (value.getString("op").equals(Envelope.Operation.READ.code())) {
                    snapshotFound = true;
                    VerifyRecord.isValidRead(record, "ID", i);
                }
                else {
                    streamingFound = true;
                    VerifyRecord.isValidInsert(record, "ID", i);
                }
            }

            // Verify that we got records from both phases
            assertThat(snapshotFound).isTrue();
            assertThat(streamingFound).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5085");
        }
    }

    @Test
    @FixFor("DBZ-4842")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to LogMiner")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Hybrid strategy now detects and handles this use case")
    public void shouldRestartAfterCapturedTableIsDroppedWhileConnectorDown() throws Exception {
        TestHelper.dropTable(connection, "dbz4842");
        try {
            connection.execute("CREATE TABLE dbz4842 (id numeric(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz4842");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4842")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4842 (id,name) values (1,'Test')");

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4842")).hasSize(1);

            stopConnector((running) -> assertThat(running).isFalse());

            connection.execute("INSERT INTO dbz4842 (id,name) values (2,'Test')");
            TestHelper.dropTable(connection, "dbz4842");

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // there won't be any records because the drop of the table will cause LogMiner to register
            // the final insert with table-name OBJ#xxxxx since the object no longer exists.
            Awaitility.await().pollDelay(10, TimeUnit.SECONDS).timeout(11, TimeUnit.SECONDS).until(() -> {
                assertNoRecordsToConsume();
                return true;
            });
        }
        finally {
            TestHelper.dropTable(connection, "dbz4842");
        }
    }

    @Test
    @FixFor("DBZ-4852")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "User-defined types not supported")
    public void shouldCaptureChangeForTableWithUnsupportedColumnType() throws Exception {
        TestHelper.dropTable(connection, "dbz4852");
        try {

            // Setup a special directory reference used by the BFILENAME arguments
            try (OracleConnection admin = TestHelper.adminConnection(false)) {
                admin.execute("CREATE OR REPLACE DIRECTORY DIR_DBZ4852 AS '/home/oracle'");
            }

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4852")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Setup table with an unsupported column type, i.e. BFILE
            connection.execute("CREATE TABLE dbz4852 (id numeric(9,0) primary key, filename bfile)");
            TestHelper.streamTable(connection, "dbz4852");

            // Perform DML operations
            connection.execute("INSERT INTO dbz4852 (id,filename) values (1,bfilename('DIR_DBZ4852','test.txt'))");
            connection.execute("UPDATE dbz4852 set filename = bfilename('DIR_DBZ4852','test2.txt') WHERE id = 1");
            connection.execute("DELETE FROM dbz4852 where id = 1");

            SourceRecords records = consumeRecordsByTopic(3);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4852")).hasSize(3);

            SourceRecord insert = records.recordsForTopic("server1.DEBEZIUM.DBZ4852").get(0);
            VerifyRecord.isValidInsert(insert, "ID", 1);

            Struct after = ((Struct) insert.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("FILENAME")).isNull();

            SourceRecord update = records.recordsForTopic("server1.DEBEZIUM.DBZ4852").get(1);
            VerifyRecord.isValidUpdate(update, "ID", 1);

            Struct before = ((Struct) update.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.schema().field("FILENAME")).isNull();

            after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("FILENAME")).isNull();

            SourceRecord delete = records.recordsForTopic("server1.DEBEZIUM.DBZ4852").get(2);
            VerifyRecord.isValidDelete(delete, "ID", 1);

            before = ((Struct) delete.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.schema().field("FILENAME")).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4852");
        }
    }

    @Test
    @FixFor("DBZ-4853")
    public void shouldCaptureChangeForTableWithUnsupportedColumnTypeLong() throws Exception {
        TestHelper.dropTable(connection, "dbz4853");
        try {
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4853")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Setup table with an unsupported column type, i.e. BFILE
            connection.execute("CREATE TABLE dbz4853 (id numeric(9,0) primary key, long_val long)");
            TestHelper.streamTable(connection, "dbz4853");

            // Perform DML operations
            connection.execute("INSERT INTO dbz4853 (id,long_val) values (1,'test.txt')");
            connection.execute("UPDATE dbz4853 set long_val = 'test2.txt' WHERE id = 1");
            connection.execute("DELETE FROM dbz4853 where id = 1");

            SourceRecords records = consumeRecordsByTopic(3);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4853")).hasSize(3);

            SourceRecord insert = records.recordsForTopic("server1.DEBEZIUM.DBZ4853").get(0);
            VerifyRecord.isValidInsert(insert, "ID", 1);

            Struct after = ((Struct) insert.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("LONG_VAL")).isNull();

            SourceRecord update = records.recordsForTopic("server1.DEBEZIUM.DBZ4853").get(1);
            VerifyRecord.isValidUpdate(update, "ID", 1);

            Struct before = ((Struct) update.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.schema().field("LONG_VAL")).isNull();

            after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().field("LONG_VAL")).isNull();

            SourceRecord delete = records.recordsForTopic("server1.DEBEZIUM.DBZ4853").get(2);
            VerifyRecord.isValidDelete(delete, "ID", 1);

            before = ((Struct) delete.value()).getStruct(Envelope.FieldName.BEFORE);
            assertThat(before.schema().field("LONG_VAL")).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4853");
        }
    }

    @Test
    @FixFor("DBZ-4907")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
    public void shouldContinueToUpdateOffsetsEvenWhenTableIsNotChanged() throws Exception {
        TestHelper.dropTable(connection, "dbz4907");
        try {

            connection.execute("CREATE TABLE dbz4907 (id numeric(9,0) primary key, state varchar2(50))");
            connection.execute("INSERT INTO dbz4907 (id,state) values (1, 'snapshot')");
            TestHelper.streamTable(connection, "dbz4907");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4907")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> table = records.recordsForTopic("server1.DEBEZIUM.DBZ4907");
            assertThat(table).hasSize(1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // There should at least be a commit by the flush policy that triggers the advancement
            // of the SCN values in the offsets within a few seconds of the polling mechanism.
            final BigInteger offsetScn = getStreamingMetric("OffsetScn");
            final BigInteger committedScn = getStreamingMetric("CommittedScn");
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
                final BigInteger newOffsetScn = getStreamingMetric("OffsetScn");
                final BigInteger newCommittedScn = getStreamingMetric("CommittedScn");
                return newOffsetScn != null &&
                        newCommittedScn != null &&
                        !newOffsetScn.equals(offsetScn) &&
                        !newCommittedScn.equals(committedScn);
            });
        }
        finally {
            TestHelper.dropTable(connection, "dbz4907");
        }
    }

    @Test
    @FixFor("DBZ-4936")
    public void shouldNotEmitLastCommittedTransactionEventsUponRestart() throws Exception {
        TestHelper.dropTable(connection, "dbz4936");
        try {
            connection.execute("CREATE TABLE dbz4936 (id numeric(9,0) primary key, name varchar2(100))");
            TestHelper.streamTable(connection, "dbz4936");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4936")
                    // this explicitly only applies when memory buffer is being used
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, "memory")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // Initiate a second connection with an open transaction
            try (OracleConnection activeTransaction = TestHelper.testConnection()) {
                // Wait for snapshot phase to conclude
                waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

                // Initiate in-progress transaction on separate connection
                activeTransaction.setAutoCommit(false);
                activeTransaction.executeWithoutCommitting("INSERT INTO dbz4936 (id,name) values (1,'In-Progress')");

                // Wait for streaming to begin
                waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

                // Perform a DML on main connection & commit
                connection.execute("INSERT INTO dbz4936 (id,name) values (2, 'committed')");

                // Get the DML
                SourceRecords records = consumeRecordsByTopic(1);
                List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ4936");
                assertThat(tableRecords).hasSize(1);
                VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 2);

                // Now update the inserted entry
                connection.execute("UPDATE dbz4936 set name = 'updated' WHERE id = 2");

                // Get the DML
                records = consumeRecordsByTopic(1);
                tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ4936");
                assertThat(tableRecords).hasSize(1);
                VerifyRecord.isValidUpdate(tableRecords.get(0), "ID", 2);

                // Stop the connector
                // This should flush commit_scn but leave scn as a value that comes before the in-progress transaction
                stopConnector();

                // Restart connector
                start(OracleConnector.class, config);
                assertConnectorIsRunning();

                // Wait for streaming & then commit in-progress transaction
                waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
                activeTransaction.commit();

                // Get the DML
                records = consumeRecordsByTopic(1);
                tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ4936");
                assertThat(tableRecords).hasSize(1);
                VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);

                // There should be no more records to consume
                assertNoRecordsToConsume();
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz4936");
        }
    }

    @Test
    @FixFor("DbZ-3318")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldSuccessfullyConnectAndStreamWithDatabaseUrl() throws Exception {
        connection.execute("INSERT INTO customer (id,name,score) values (1001, 'DBZ3668', 100)");

        // Use the default configuration as a baseline.
        // The default configuration automatically adds the `database.hostname` property, but we want to
        // use `database.url` instead. So we'll generate a map, remove the hostname key and then add
        // the url key instead before creating a new configuration object for the connector.
        final Map<String, String> defaultConfig = TestHelper.defaultConfig().build().asMap();
        defaultConfig.remove(OracleConnectorConfig.HOSTNAME.name());
        defaultConfig.put(OracleConnectorConfig.URL.name(), TestHelper.getOracleConnectionUrlDescriptor());

        Configuration.Builder builder = Configuration.create();
        for (Map.Entry<String, String> entry : defaultConfig.entrySet()) {
            builder.with(entry.getKey(), entry.getValue());
        }

        start(OracleConnector.class, builder.build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Read snapshot record
        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(tableRecords).hasSize(1);
        VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1001);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Test
    @FixFor("DBZ-4953")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldStreamTruncateEventWhenLobIsEnabled() throws Exception {
        TestHelper.dropTable(connection, "dbz4953");
        try {
            connection.execute("CREATE TABLE dbz4953 (id numeric(9,0) primary key, col2 varchar2(100))");
            TestHelper.streamTable(connection, "dbz4953");
            connection.execute("INSERT INTO dbz4953 (id,col2) values (1, 'Daffy Duck')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4953")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .with(OracleConnectorConfig.SKIPPED_OPERATIONS, "none") // do not skip truncates
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ4953");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);

            // truncate
            connection.execute("TRUNCATE TABLE dbz4953");

            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ4953");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidTruncate(tableRecords.get(0));
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4953");
        }
    }

    @Test
    @FixFor("DBZ-4963")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldRestartLogMiningSessionAfterMaxSessionElapses() throws Exception {
        TestHelper.dropTable(connection, "dbz4963");
        try {
            connection.execute("CREATE TABLE dbz4963 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz4963");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4963")
                    .with(OracleConnectorConfig.LOG_MINING_SESSION_MAX_MS, 10_000L)
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start a transaction so that the low SCN never advances
            // When no transactions are active, the session is closed and the positions are advanced
            connection.executeWithoutCommitting("INSERT INTO dbz4963 values (1, 'test')");

            // Wait at most 60 seconds until we get notified in the logs that maximum session had exceeded.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> logInterceptor.containsMessage("LogMiner session has exceeded maximum session time"));

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4963");
        }
    }

    @Test
    @FixFor("DBZ-4963")
    @Ignore("Waits 60 seconds by default, so disabled by default")
    public void shouldNotRestartLogMiningSessionWithMaxSessionZero() throws Exception {
        TestHelper.dropTable(connection, "dbz4963");
        try {
            connection.execute("CREATE TABLE dbz4963 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz4963");

            // default max session is 0L
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4963")
                    .build();

            LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try {
                // Wait at most 60 seconds for potential message
                // The message should never be written here and so a ConditionTimeoutException should be thrown.
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .until(() -> logInterceptor.containsMessage("LogMiner session has exceeded maximum session time"));
            }
            catch (ConditionTimeoutException e) {
                // expected
                stopConnector();
                return;
            }

            fail("Expected a ConditionTimeoutException, LogMiner session max session message should not have been written.");
        }
        finally {
            TestHelper.dropTable(connection, "dbz4963");
        }
    }

    @Test
    @FixFor("DBZ-5006")
    public void shouldSupportTablesWithForwardSlashes() throws Exception {
        // Different forward-slash scenarios
        testTableWithForwardSlashes("/dbz5006", "_dbz5006");
        testTableWithForwardSlashes("dbz/5006", "dbz_5006");
        testTableWithForwardSlashes("dbz5006/", "dbz5006_");
        testTableWithForwardSlashes("db/z50/06", "db_z50_06");
        testTableWithForwardSlashes("dbz//5006", "dbz__5006");
    }

    @Test
    @FixFor("DBZ-5119")
    public void shouldExecuteHeartbeatActionQuery() throws Exception {
        TestHelper.dropTable(connection, "dbz5119");
        TestHelper.dropTable(connection, "heartbeat");
        try {
            connection.execute("CREATE TABLE heartbeat (data timestamp)");
            connection.execute("INSERT INTO heartbeat values (sysdate)");

            TestHelper.grantRole("INSERT,UPDATE", "debezium.heartbeat", TestHelper.getConnectorUserName());

            connection.execute("CREATE TABLE dbz5119 (id numeric(9,0) primary key, data varchar2(50))");

            TestHelper.streamTable(connection, "dbz5119");
            TestHelper.streamTable(connection, "heartbeat");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, "no_data")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5119,DEBEZIUM\\.HEARTBEAT")
                    .with(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY, "UPDATE debezium.heartbeat set data = sysdate WHERE ROWNUM = 1")
                    .with(DatabaseHeartbeatImpl.HEARTBEAT_INTERVAL, 1000)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
                final SourceRecords records = consumeRecordsByTopic(1);
                final List<SourceRecord> heartbeatRecords = records.recordsForTopic("server1.DEBEZIUM.HEARTBEAT");
                return heartbeatRecords != null && !heartbeatRecords.isEmpty();
            });

            // stop connector and clean-up any potential residual heartbeat events
            stopConnector((success) -> {
                consumeAvailableRecords(r -> {
                });
            });
        }
        finally {
            TestHelper.dropTable(connection, "heartbeat");
            TestHelper.dropTable(connection, "dbz5119");
        }
    }

    @Test
    @FixFor("DBZ-5147")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to Oracle LogMiner implementation")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Test overrides strategy as requires online_catalog")
    public void shouldStopWhenErrorProcessingFailureHandlingModeIsDefault() throws Exception {
        TestHelper.dropTable(connection, "dbz5147");
        try {
            connection.execute("CREATE TABLE dbz5147 (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("INSERT INTO dbz5147 VALUES (1, 'test1')");
            TestHelper.streamTable(connection, "dbz5147");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5147")
                    // We explicitly use this mode here to enforce the schema evolution issues with DML
                    // event failures because Oracle LogMiner doesn't track DDL evolution.
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5147");
            assertThat(records).hasSize(1);
            VerifyRecord.isValidRead(records.get(0), "ID", 1);

            // We want to stop the connector and perform several DDL operations offline
            // This will cause some DML events that happen offline to not be parsed by Oracle LogMiner.
            stopConnector();

            connection.execute("ALTER TABLE dbz5147 add data2 varchar2(50)");
            connection.execute("INSERT INTO dbz5147 values (2, 'test2a', 'test2b')");
            connection.execute("ALTER TABLE dbz5147 drop column data2");
            connection.execute("INSERT INTO dbz5147 values (3, 'test3')");

            final LogInterceptor interceptor = TestHelper.getAbstractEventProcessorLogInterceptor();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // wait and confirm that the exception is thrown.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> interceptor.containsErrorMessage(ERROR_PROCESSING_FAIL_MESSAGE));

            // there should be no records to consume since we hard-failed
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5147");
        }
    }

    @Test
    @FixFor("DBZ-5147")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to Oracle LogMiner implementation")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Test overrides strategy as requires online_catalog")
    public void shouldLogWarningAndSkipWhenErrorProcessingFailureHandlingModeIsWarn() throws Exception {
        TestHelper.dropTable(connection, "dbz5147");
        try {
            connection.execute("CREATE TABLE dbz5147 (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("INSERT INTO dbz5147 VALUES (1, 'test1')");
            TestHelper.streamTable(connection, "dbz5147");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5147")
                    .with(OracleConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "warn")
                    // We explicitly use this mode here to enforce the schema evolution issues with DML
                    // event failures because Oracle LogMiner doesn't track DDL evolution.
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5147");
            assertThat(records).hasSize(1);
            VerifyRecord.isValidRead(records.get(0), "ID", 1);

            // We want to stop the connector and perform several DDL operations offline
            // This will cause some DML events that happen offline to not be parsed by Oracle LogMiner.
            stopConnector();

            connection.execute("ALTER TABLE dbz5147 add data2 varchar2(50)");
            connection.execute("INSERT INTO dbz5147 values (2, 'test2a', 'test2b')");
            connection.execute("ALTER TABLE dbz5147 drop column data2");
            connection.execute("INSERT INTO dbz5147 values (3, 'test3')");

            final LogInterceptor interceptor = TestHelper.getAbstractEventProcessorLogInterceptor();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // wait and confirm that the exception is thrown.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> interceptor.containsWarnMessage(ERROR_PROCESSING_WARN_MESSAGE));

            // There should only be one record to consume, the valid insert for ID=3.
            // The record for ID=2 will have yielded a WARN log entry we checked above.
            sourceRecords = consumeRecordsByTopic(1);
            records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5147");
            assertThat(records).hasSize(1);
            VerifyRecord.isValidInsert(records.get(0), "ID", 3);
        }
        finally {
            TestHelper.dropTable(connection, "dbz5147");
        }
    }

    @Test
    @FixFor("DBZ-5147")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Only applies to Oracle LogMiner implementation")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Test overrides strategy as requires online_catalog")
    public void shouldSilentlySkipWhenErrorProcessingFailureHandlingModeIsSkip() throws Exception {
        TestHelper.dropTable(connection, "dbz5147");
        try {
            connection.execute("CREATE TABLE dbz5147 (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("INSERT INTO dbz5147 VALUES (1, 'test1')");
            TestHelper.streamTable(connection, "dbz5147");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5147")
                    .with(OracleConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, "skip")
                    // We explicitly use this mode here to enforce the schema evolution issues with DML
                    // event failures because Oracle LogMiner doesn't track DDL evolution.
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5147");
            assertThat(records).hasSize(1);
            VerifyRecord.isValidRead(records.get(0), "ID", 1);

            // We want to stop the connector and perform several DDL operations offline
            // This will cause some DML events that happen offline to not be parsed by Oracle LogMiner.
            stopConnector();

            connection.execute("ALTER TABLE dbz5147 add data2 varchar2(50)");
            connection.execute("INSERT INTO dbz5147 values (2, 'test2a', 'test2b')");
            connection.execute("ALTER TABLE dbz5147 drop column data2");
            connection.execute("INSERT INTO dbz5147 values (3, 'test3')");

            final LogInterceptor interceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // There should only be one record to consume, the valid insert for ID=3.
            // The record for ID=2 will have simply been skipped due to STATUS=2.
            sourceRecords = consumeRecordsByTopic(1);
            records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ5147");
            assertThat(records).hasSize(1);
            VerifyRecord.isValidInsert(records.get(0), "ID", 3);

            // No errors/warnings should have been logged for ID=2 record with STATUS=2.
            assertThat(interceptor.containsErrorMessage(ERROR_PROCESSING_FAIL_MESSAGE)).isFalse();
            assertThat(interceptor.containsWarnMessage(ERROR_PROCESSING_WARN_MESSAGE)).isFalse();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5147");
        }
    }

    @Test
    @FixFor({ "DBZ-5139", "DBZ-8880" })
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
    public void shouldDiscardTransactionThatExceedsEventThreshold() throws Exception {
        TestHelper.dropTable(connection, "dbz5139");
        try {
            connection.execute("CREATE TABLE dbz5139 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5139");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5139")
                    .with(OracleConnectorConfig.LOG_MINING_BUFFER_TRANSACTION_EVENTS_THRESHOLD, 100)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            // create a transaction that exceeds the threshold
            for (int i = 0; i < 101; ++i) {
                connection.executeWithoutCommitting("INSERT INTO dbz5139 (id,data) values (" + i + ", 'Test" + i + "')");
            }
            connection.commit();

            // Create a transaction that does not exceed the threshold
            for (int i = 200; i < 225; ++i) {
                connection.executeWithoutCommitting("INSERT INTO dbz5139 (id,data) values (" + i + ", 'Test" + i + "')");
            }
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(25);
            List<SourceRecord> table = records.recordsForTopic("server1.DEBEZIUM.DBZ5139");
            assertThat(table).hasSize(25);

            for (int i = 0; i < 25; ++i) {
                VerifyRecord.isValidInsert(table.get(i), "ID", 200 + i);
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5139");
        }
    }

    @Test
    @FixFor("DBZ-5139")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldNotDiscardTransactionWhenNoEventThresholdSet() throws Exception {
        TestHelper.dropTable(connection, "dbz5139");
        try {
            connection.execute("CREATE TABLE dbz5139 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5139");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5139")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            // create a transaction that exceeds the threshold
            for (int i = 0; i < 101; ++i) {
                connection.executeWithoutCommitting("INSERT INTO dbz5139 (id,data) values (" + i + ", 'Test" + i + "')");
            }
            connection.commit();

            // Create a transaction that does not exceed the threshold
            for (int i = 200; i < 225; ++i) {
                connection.executeWithoutCommitting("INSERT INTO dbz5139 (id,data) values (" + i + ", 'Test" + i + "')");
            }
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(126);
            List<SourceRecord> table = records.recordsForTopic("server1.DEBEZIUM.DBZ5139");
            assertThat(table).hasSize(126);

            for (int i = 0; i < 101; ++i) {
                VerifyRecord.isValidInsert(table.get(i), "ID", i);
            }

            for (int i = 0; i < 25; ++i) {
                VerifyRecord.isValidInsert(table.get(101 + i), "ID", 200 + i);
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5139");
        }
    }

    @Test
    @FixFor("DBZ-5356")
    public void shouldUniqueIndexWhenAtLeastOneColumnIsExcluded() throws Exception {
        TestHelper.dropTable(connection, "dbz5356");
        try {
            connection.execute("CREATE TABLE dbz5356 (id numeric(9,0), data varchar2(50))");
            connection.execute("CREATE UNIQUE INDEX dbz5356_idx ON dbz5356 (upper(data), id)");
            TestHelper.streamTable(connection, "dbz5356");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5356")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5356");
        }
    }

    private void testTableWithForwardSlashes(String tableName, String topicTableName) throws Exception {
        final String quotedTableName = "\"" + tableName + "\"";
        TestHelper.dropTable(connection, quotedTableName);
        try {
            // Always want to make sure the offsets are cleared for each invocation of this sub-test
            Testing.Files.delete(OFFSET_STORE_PATH);
            Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

            connection.execute("CREATE TABLE " + quotedTableName + " (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("INSERT INTO " + quotedTableName + " (id,data) values (1, 'Record1')");
            TestHelper.streamTable(connection, quotedTableName);

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\." + tableName)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // NOTE: Forward slashes are not valid in topic names, will be converted to underscore.
            // The following takes this into account.
            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM." + topicTableName);
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            connection.execute("INSERT INTO " + quotedTableName + " (id,data) values (2,'Record2')");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM." + topicTableName);
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 2);

            connection.execute("UPDATE " + quotedTableName + " SET data = 'Record2u' WHERE id = 2");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM." + topicTableName);
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidUpdate(tableRecords.get(0), "ID", 2);

            connection.execute("DELETE " + quotedTableName + " WHERE id = 1");
            records = consumeRecordsByTopic(2);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM." + topicTableName);
            assertThat(tableRecords).hasSize(2);
            VerifyRecord.isValidDelete(tableRecords.get(0), "ID", 1);
            VerifyRecord.isValidTombstone(tableRecords.get(1));

            assertNoRecordsToConsume();
        }
        catch (Exception e) {
            throw new RuntimeException("Forward-slash test failed for table: " + tableName, e);
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, quotedTableName);
        }
    }

    @Test
    @FixFor("DBZ-5441")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "binary objects are skipped")
    public void shouldGracefullySkipObjectBasedTables() throws Exception {
        TestHelper.dropTable(connection, "dbz5441");
        try {
            // This grant isn't given by default, but it is needed to create types in this test case.
            TestHelper.grantRole("CREATE ANY TYPE");

            // Sets up all the log interceptors needed
            final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);

            // Setup object type and object table
            connection.execute("CREATE TYPE DEBEZIUM.DBZ5441_TYPE AS OBJECT (id number, lvl number)");
            connection.execute("CREATE TABLE DEBEZIUM.DBZ5441 of DEBEZIUM.DBZ5441_TYPE (primary key(id))");
            TestHelper.streamTable(connection, "DBZ5441");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5441")
                    .build();

            int waitTime = TestHelper.defaultMessageConsumerPollTimeout() * 2;

            final LogInterceptor streamInterceptor = TestHelper.getAbstractEventProcessorLogInterceptor();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            // Simply indicates we did not find the table to lock and capture
            // In other words, the snapshot performs no action on this table
            assertThat(logInterceptor.containsMessage("Locking captured tables []")).isTrue();

            connection.execute("INSERT INTO DEBEZIUM.DBZ5441 (id,lvl) values (1,1)");

            Awaitility.await()
                    .atMost(waitTime, TimeUnit.SECONDS)
                    .until(() -> streamInterceptor.containsMessage("not a relational table. The event will be skipped."));

            assertNoRecordsToConsume();
            stopConnector();
        }
        finally {
            // Drop table and type
            TestHelper.dropTable(connection, "dbz5441");
            connection.execute("DROP TYPE DEBEZIUM.DBZ5441_TYPE");
            // Revoke special role granted for this test case
            TestHelper.revokeRole("CREATE ANY TYPE");
        }
    }

    @Test
    @FixFor("DBZ-5682")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldCaptureChangesForTableUniqueIndexWithNullColumnValuesWhenLobEnabled() throws Exception {
        TestHelper.dropTable(connection, "dbz5682");
        try {
            connection.execute("CREATE TABLE dbz5682 (col_bpchar varchar2(30), col_varchar varchar2(30), col_int4 number(5), " +
                    "constraint uk_dbz5862 unique (col_bpchar, col_varchar))");
            TestHelper.streamTable(connection, "dbz5682");

            connection.execute("INSERT INTO dbz5682 values ('1', null, 1)");

            // This test requires that LOB_ENABLED be set to true in order to trigger the failure behavior,
            // which was not expecting that a primary key column's value would ever be NULL, but when the
            // table uses a unique index, like this example, it's possible.
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5682")
                    .with(OracleConnectorConfig.LOB_ENABLED, true)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Snapshot records
            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DBZ5682");
            assertThat(recordsForTopic).hasSize(1);

            Struct after = ((Struct) recordsForTopic.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("COL_BPCHAR")).isEqualTo("1");
            assertThat(after.get("COL_VARCHAR")).isNull();
            assertThat(after.get("COL_INT4")).isEqualTo(1);

            connection.execute("INSERT INTO dbz5682 values ('2', null, 2)");

            // Streaming records
            records = consumeRecordsByTopic(1);

            recordsForTopic = records.recordsForTopic("server1.DEBEZIUM.DBZ5682");
            assertThat(recordsForTopic).hasSize(1);

            after = ((Struct) recordsForTopic.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("COL_BPCHAR")).isEqualTo("2");
            assertThat(after.get("COL_VARCHAR")).isNull();
            assertThat(after.get("COL_INT4")).isEqualTo(2);
        }
        finally {
            TestHelper.dropTable(connection, "dbz5682");
        }
    }

    @Test
    @FixFor("DBZ-5626")
    public void shouldNotUseOffsetScnWhenSnapshotIsAlways() throws Exception {
        try {
            Configuration.Builder builder = defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5626");
            Configuration config = builder.build();

            TestHelper.dropTable(connection, "DBZ5626");
            connection.execute("CREATE TABLE DBZ5626 (ID number(9,0), DATA varchar2(50))");
            TestHelper.streamTable(connection, "DBZ5626");
            connection.execute("INSERT INTO DBZ5626 (ID, DATA) values (1, 'Test1')", "INSERT INTO DBZ5626 (ID, DATA) values (2, 'Test2')");

            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            int expectedRecordCount = 2;
            SourceRecords sourceRecords = consumeRecordsByTopic(expectedRecordCount);
            assertThat(sourceRecords.allRecordsInOrder()).hasSize(expectedRecordCount);
            Struct struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(1, struct.get("ID"));
            assertEquals("Test1", struct.get("DATA"));
            stopConnector();

            // To verify later on that we do snapshot from up-to-date SCN and not from one stored in the offset, delete one row.
            connection.execute("DELETE FROM DBZ5626 WHERE ID=1");
            connection.execute("INSERT INTO DBZ5626 (ID, DATA) values (3, 'Test3')");

            start(OracleConnector.class, config);
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            sourceRecords = consumeRecordsByTopic(expectedRecordCount);

            // Check we get up-to-date data in the snapshot.
            assertThat(sourceRecords.allRecordsInOrder()).hasSize(expectedRecordCount);
            struct = (Struct) ((Struct) sourceRecords.allRecordsInOrder().get(0).value()).get(AFTER);
            assertEquals(2, struct.get("ID"));
            assertEquals("Test2", struct.get("DATA"));
        }
        finally {
            TestHelper.dropTable(connection, "DBZ5626");
        }
    }

    @Test
    @FixFor("DBZ-5738")
    public void shouldSkipSnapshotOfNestedTable() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);

        TestHelper.dropTable(connection, "DBZ5738");
        TestHelper.grantRole("CREATE ANY TYPE");

        try {
            String myTableTypeDll = "CREATE OR REPLACE TYPE my_tab_t AS TABLE OF VARCHAR2(128);";
            connection.execute(myTableTypeDll);

            String nestedTableDdl = "create table DBZ5738 (" +
                    " id numeric(9,0) not null, " +
                    " c1 int, " +
                    " c2 my_tab_t, " +
                    " primary key (id)) " +
                    " nested table c2 store as nested_table";
            connection.execute(nestedTableDdl);

            TestHelper.streamTable(connection, "DBZ5738");
            connection.execute("INSERT INTO DBZ5738 VALUES (1, 25, my_tab_t('test1'))");
            connection.execute("INSERT INTO DBZ5738 VALUES (2, 50, my_tab_t('test2'))");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5738")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify that the table is not snapshotted and list of the tables is empty.
            assertNoRecordsToConsume();
            assertThat(logInterceptor.containsMessage("Locking captured tables []")).isTrue();

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "DBZ5738");
        }
    }

    @Test
    @FixFor("DBZ-5907")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "LogMiner only")
    public void shouldUndoOnlyLastEventWithSavepoint() throws Exception {
        TestHelper.dropTable(connection, "dbz5907");
        try {

            connection.execute("CREATE TABLE dbz5907 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5907");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5907")
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz5907 (id,data) values (1, 'insert')");

            // Assert initial inserted record
            SourceRecords tableRecords = consumeRecordsByTopic(1);
            assertThat(tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907")).hasSize(1);
            SourceRecord record = tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907").get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);
            assertThat(getAfter(record).get("DATA")).isEqualTo("insert");

            // Perform an update insert, followed by savepoint and an update that gets rolled back to savepoint
            connection.execute("BEGIN " +
                    "UPDATE dbz5907 SET data = 'update' WHERE id = 1;" +
                    "INSERT INTO dbz5907 (id,data) values (2, 'insert');" +
                    "SAVEPOINT a;" +
                    "UPDATE dbz5907 SET data = 'updateb' WHERE id = 1;" +
                    "ROLLBACK TO SAVEPOINT a;" +
                    "COMMIT;" +
                    "END;");

            // Assert update record
            tableRecords = consumeRecordsByTopic(2);
            assertThat(tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907")).hasSize(2);
            record = tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907").get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);
            assertThat(getAfter(record).get("DATA")).isEqualTo("update");

            // Assert insert record
            record = tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907").get(1);
            VerifyRecord.isValidInsert(record, "ID", 2);
            assertThat(getAfter(record).get("DATA")).isEqualTo("insert");

            // There should be no records left to consume
            assertNoRecordsToConsume();

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5907");
        }
    }

    @Test
    @FixFor("DBZ-5907")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "LogMiner only")
    public void shouldCorrectlyUndoWithMultipleSavepoints() throws Exception {
        TestHelper.dropTable(connection, "dbz5907");
        try {

            connection.execute("CREATE TABLE dbz5907 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz5907");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ5907")
                    .build();

            final LogInterceptor interceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz5907 (id,data) values (1, 'insert')");

            // Assert initial inserted record
            SourceRecords tableRecords = consumeRecordsByTopic(1);
            assertThat(tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907")).hasSize(1);
            SourceRecord record = tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907").get(0);
            VerifyRecord.isValidInsert(record, "ID", 1);
            assertThat(getAfter(record).get("DATA")).isEqualTo("insert");

            // Perform an update insert, followed by savepoint and an update that gets rolled back to savepoint
            connection.execute("BEGIN " +
                    "SAVEPOINT a;" +
                    "UPDATE dbz5907 SET data = 'update' WHERE id = 1;" +
                    "INSERT INTO dbz5907 (id,data) values (2, 'insert');" +
                    "SAVEPOINT b;" +
                    "UPDATE dbz5907 SET data = 'updateb' WHERE id = 1;" +
                    "ROLLBACK TO SAVEPOINT b;" +
                    "ROLLBACK TO SAVEPOINT a;" +
                    "UPDATE dbz5907 SET data = 'updatea' WHERE id = 1;" +
                    "COMMIT;" +
                    "END;");

            // Assert update record
            tableRecords = consumeRecordsByTopic(1);
            assertThat(tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907")).hasSize(1);
            record = tableRecords.recordsForTopic("server1.DEBEZIUM.DBZ5907").get(0);
            VerifyRecord.isValidUpdate(record, "ID", 1);
            assertThat(getAfter(record).get("DATA")).isEqualTo("updatea");

            // There should be no records left to consume
            assertNoRecordsToConsume();

            stopConnector();

            assertThat(interceptor.containsMessage("Cannot apply undo change"))
                    .as("Unable to correctly undo operation within transaction")
                    .isFalse();

            assertThat(interceptor.containsMessage("Failed to apply undo change"))
                    .as("Unable to apply undo operation")
                    .isFalse();
        }
        finally {
            TestHelper.dropTable(connection, "dbz5907");
        }
    }

    @Test
    @FixFor("DBZ-6107")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldNotConsolidateBulkUpdateWhenLobEnabledIfUpdatesAreDifferentLogicalRowsWithoutLobColumns() throws Exception {
        TestHelper.dropTable(connection, "dbz6107");
        try {
            connection.execute("CREATE TABLE dbz6107 (a numeric(9,0), b varchar2(25))");
            TestHelper.streamTable(connection, "dbz6107");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6107")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            for (int i = 1; i <= 10; i++) {
                connection.execute("INSERT INTO dbz6107 (a,b) values (" + i + ",'t" + i + "')");
            }
            connection.execute("UPDATE dbz6107 SET a=12 WHERE a=1 OR a=2");

            SourceRecords records = consumeRecordsByTopic(12);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6107");
            assertThat(tableRecords).hasSize(12);

            for (int i = 1; i <= 10; i++) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidInsert(record);
                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(i);
                assertThat(after.get("B")).isEqualTo("t" + i);
            }

            for (int i = 11; i <= 12; ++i) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidUpdate(record);

                final Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
                assertThat(before.get("A")).isEqualTo((i - 10));
                assertThat(before.get("B")).isEqualTo("t" + (i - 10));

                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(12);
                assertThat(after.get("B")).isEqualTo("t" + (i - 10));
            }

            assertNoRecordsToConsume();
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, "dbz6107");
        }
    }

    @Test
    @FixFor("DBZ-6107")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void shouldNotConsolidateBulkUpdateWhenLobEnabledIfUpdatesAreDifferentLogicalRowsWithLobColumns() throws Exception {
        TestHelper.dropTable(connection, "dbz6107");
        try {
            connection.execute("CREATE TABLE dbz6107 (a numeric(9,0), b varchar2(25), d clob, c clob)");
            TestHelper.streamTable(connection, "dbz6107");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6107")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Perform 10 individual inserts
            for (int i = 1; i <= 10; i++) {
                connection.execute("INSERT INTO dbz6107 (a,b,c,d) values (" + i + ",'t" + i + "', 'data" + i + "','x')");
            }

            // This bulk update only adjusts two rows where the value changed is a non-LOB column.
            // Oracle will emit 2 independent UPDATE events, which should be emitted separately.
            connection.execute("UPDATE dbz6107 SET a=12 WHERE a=1 OR a=2");

            // This bulk update also adjusts two rows, but this changes both a non-LOB and LOB column.
            // In this case, Oracle will emit 4 UPDATE events, where the first two and last two should
            // be combined. These 4 events look like as follows:
            //
            // Event 1 - Non-LOB column a=3 changed to a=13
            // Event 2 - LOB column c changed to 'Updated' where a=13 and b=t3 (previously where a=3)
            // Event 3 - Non-LOB column a=4 changed to a=13
            // Event 4 - LOB column c changed to 'Updated' where a=13 and b=t4 (previously where a=4)
            //
            // The outcome should be a total of 2 unique events, both updates, where the contents
            // of the events should be the following:
            //
            // Event 1 - a=13, b=t3, c='Updated'
            // Event 2 - a=13, b=t4, c='Updated'
            connection.execute("UPDATE dbz6107 SET a=13, c = 'Updated' WHERE a=3 OR a=4");

            // This bulk update also adjusts two rows, but this changes both a non-LOB and LOB column.
            // In this case, Oracle will emit 4 UPDATE events, where the first two and last two should
            // be combined. These 4 events look like as follows:
            //
            // Event 1 - Non-LOB column a=35 changed to a=14
            // Event 2 - LOB column c changed to NULL where a=14 and b=t5 (previously where a=5)
            // Event 3 - Non-LOB column a=6 changed to a=14
            // Event 4 - LOB column c changed to NULL where a=14 and b=t6 (previously where a=6)
            //
            // The outcome should be a total of 2 unique events, both updates, where the contents
            // of the events should be the following:
            //
            // Event 1 - a=13, b=t3, c=null
            // Event 2 - a=13, b=t4, c=null
            connection.execute("UPDATE dbz6107 SET a=14, c = NULL WHERE a=5 OR a=6");

            final int count = 10 + 2 + 2 + 2;
            SourceRecords records = consumeRecordsByTopic(count);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6107");
            assertThat(tableRecords).hasSize(count);

            // Check initial 10 inserts have all 3 columns populated
            for (int i = 1; i <= 10; i++) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidInsert(record);

                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(i);
                assertThat(after.get("B")).isEqualTo("t" + i);
                assertThat(after.get("C")).isEqualTo("data" + i);
            }

            // First bulk update should have produced exactly 2 update events.
            for (int i = 11; i <= 12; ++i) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidUpdate(record);

                final Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
                assertThat(before.get("A")).isEqualTo((i - 10));
                assertThat(before.get("B")).isEqualTo("t" + (i - 10));
                // An UPDATE never provides the prior LOB column data, placeholder is used
                assertThat(before.get("C")).isEqualTo("__debezium_unavailable_value");

                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(12);
                assertThat(after.get("B")).isEqualTo("t" + (i - 10));
                // This bulk UPDATE did not manipulate the LOB column, so placeholder is used
                assertThat(after.get("C")).isEqualTo("__debezium_unavailable_value");
            }

            // Second bulk update should have provided exactly 2 update events.
            for (int i = 13; i <= 14; ++i) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidUpdate(record);

                final Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
                assertThat(before.get("A")).isEqualTo((i - 10));
                assertThat(before.get("B")).isEqualTo("t" + (i - 10));
                // An UPDATE never provides the prior LOB column data, placeholder is used
                assertThat(before.get("C")).isEqualTo("__debezium_unavailable_value");

                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(13);
                assertThat(after.get("B")).isEqualTo("t" + (i - 10));
                // This bulk UPDATE did manipulate the LOB column, so value should exist
                assertThat(after.get("C")).isEqualTo("Updated");
            }

            // Second bulk update should have provided exactly 2 update events.
            for (int i = 15; i <= 16; ++i) {
                final SourceRecord record = tableRecords.get(i - 1);
                VerifyRecord.isValidUpdate(record);

                final Struct before = ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
                assertThat(before.get("A")).isEqualTo((i - 10));
                assertThat(before.get("B")).isEqualTo("t" + (i - 10));
                // An UPDATE never provides the prior LOB column data, placeholder is used
                assertThat(before.get("C")).isEqualTo("__debezium_unavailable_value");

                final Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("A")).isEqualTo(14);
                assertThat(after.get("B")).isEqualTo("t" + (i - 10));
                // This bulk UPDATE did manipulate the LOB column, so value should exist as NULL
                assertThat(after.get("C")).isNull();
            }

            assertNoRecordsToConsume();
        }
        finally {
            stopConnector();
            TestHelper.dropTable(connection, "dbz6107");
        }
    }

    @Test
    @FixFor("DBZ-6120")
    public void testCapturingChangesForTableWithSapcesInName() throws Exception {
        TestHelper.dropTable(connection, "\"Q1! 表\"");
        try {
            connection.execute("CREATE TABLE \"Q1! 表\" (a int)");
            connection.execute("INSERT INTO \"Q1! 表\" (a) values (1)");
            TestHelper.streamTable(connection, "\"Q1! 表\"");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.Q1! 表")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.Q1___")).hasSize(1);

            connection.execute("INSERT INTO \"Q1! 表\" (a) values (2)");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.Q1___")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "\"Q1! 表\"");
        }
    }

    @Test
    @FixFor("DBZ-6120")
    public void testCapturingChangesForTableWithSpecialCharactersInName() throws Exception {
        TestHelper.dropTable(connection, "\"Q1!表\"");
        try {
            connection.execute("CREATE TABLE \"Q1!表\" (a int)");
            connection.execute("INSERT INTO \"Q1!表\" (a) values (1)");
            TestHelper.streamTable(connection, "\"Q1!表\"");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.Q1!表")
                    .build();

            start(OracleConnector.class, config);
            assertNoRecordsToConsume();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.Q1__")).hasSize(1);

            connection.execute("INSERT INTO \"Q1!表\" (a) values (2)");

            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.Q1__")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "\"Q1!表\"");
        }
    }

    @Test
    @FixFor("DBZ-6143")
    public void testTimestampWithTimeZoneFormatConsistentUsingDriverEnabledTimestampTzInGmt() throws Exception {
        TestHelper.dropTable(connection, "tz_test");
        try {
            connection.execute("CREATE TABLE tz_test (a timestamp with time zone)");
            connection.execute("INSERT INTO tz_test values (to_timestamp_tz('2010-12-01 23:12:56.788 -12:44', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))");
            TestHelper.streamTable(connection, "tz_test");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.TZ_TEST")
                    .with("driver.oracle.jdbc.timestampTzInGmt", "true") // driver default
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Snapshot
            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.TZ_TEST");
            assertThat(tableRecords).hasSize(1);
            assertThat(getAfter(tableRecords.get(0)).get("A")).isEqualTo("2010-12-01T23:12:56.788000-12:44");

            // Streaming
            connection.execute("INSERT INTO tz_test values (to_timestamp_tz('2010-12-01 23:12:56.788 -12:44', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.TZ_TEST");
            assertThat(tableRecords).hasSize(1);
            assertThat(getAfter(tableRecords.get(0)).get("A")).isEqualTo("2010-12-01T23:12:56.788000-12:44");
        }
        finally {
            TestHelper.dropTable(connection, "tz_test");
        }
    }

    @Test
    @FixFor("DBZ-6143")
    public void testTimestampWithTimeZoneFormatConsistentUsingDriverDisabledTimestampTzInGmt() throws Exception {
        TestHelper.dropTable(connection, "tz_test");
        try {
            connection.execute("CREATE TABLE tz_test (a timestamp with time zone)");
            connection.execute("INSERT INTO tz_test values (to_timestamp_tz('2010-12-01 23:12:56.788 -12:44', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))");
            TestHelper.streamTable(connection, "tz_test");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.TZ_TEST")
                    .with("driver.oracle.jdbc.timestampTzInGmt", "false")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Snapshot
            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.TZ_TEST");
            assertThat(tableRecords).hasSize(1);
            assertThat(getAfter(tableRecords.get(0)).get("A")).isEqualTo("2010-12-01T23:12:56.788000-12:44");

            // Streaming
            connection.execute("INSERT INTO tz_test values (to_timestamp_tz('2010-12-01 23:12:56.788 -12:44', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'))");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.TZ_TEST");
            assertThat(tableRecords).hasSize(1);
            assertThat(getAfter(tableRecords.get(0)).get("A")).isEqualTo("2010-12-01T23:12:56.788000-12:44");
        }
        finally {
            TestHelper.dropTable(connection, "tz_test");
        }
    }

    @Test
    @FixFor("DBZ-6221")
    public void testShouldProperlyMapCharacterColumnTypesAsCharWhenTableCreatedDuringStreamingPhase() throws Exception {
        TestHelper.dropTable(connection, "dbz6221");
        try {
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ6221")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("CREATE TABLE dbz6221 (data0 character, data1 character(5), data2 character varying(5))");
            TestHelper.streamTable(connection, "dbz6221");

            connection.execute("INSERT INTO dbz6221 values ('a', 'abc', 'abc')");

            final SourceRecords records = consumeRecordsByTopic(1);

            final List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6221");
            assertThat(tableRecords).hasSize(1);

            final SourceRecord record = tableRecords.get(0);
            Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.schema().fields()).hasSize(3);
            assertThat(after.schema().field("DATA0").schema().type()).isEqualTo(Schema.Type.STRING);
            assertThat(after.schema().field("DATA1").schema().type()).isEqualTo(Schema.Type.STRING);
            assertThat(after.schema().field("DATA2").schema().type()).isEqualTo(Schema.Type.STRING);
            assertThat(after.get("DATA0")).isEqualTo("a");
            assertThat(after.get("DATA1")).isEqualTo("abc  ");
            assertThat(after.get("DATA2")).isEqualTo("abc");
        }
        finally {
            TestHelper.dropTable(connection, "dbz6221");
        }
    }

    @Test
    @FixFor("DBZ-5395")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies to LogMiner only")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void testShouldAdvanceStartScnWhenNoActiveTransactionsBetweenIterationsWhenLobEnabled() throws Exception {
        TestHelper.dropTable(connection, "dbz5395");
        try {
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM.DBZ5395")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .build();

            connection.execute("CREATE TABLE dbz5395 (data0 character, data1 character(5), data2 character varying(5))");
            TestHelper.streamTable(connection, "dbz5395");

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final AtomicReference<Scn> offsetScn = new AtomicReference<>(Scn.NULL);
            Awaitility.await().atMost(Duration.ofMinutes(5)).until(() -> {
                final BigInteger offsetScnValue = getStreamingMetric("OffsetScn");
                if (offsetScnValue != null) {
                    offsetScn.set(new Scn(offsetScnValue));
                    return true;
                }
                return false;
            });

            Awaitility.await().atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(2)).until(() -> {
                return new Scn(getStreamingMetric("OffsetScn")).compareTo(offsetScn.get()) > 0;
            });
        }
        finally {
            TestHelper.dropTable(connection, "dbz5395");
        }
    }

    @Test
    @FixFor("DBZ-6355")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED, reason = "User-defined types not supported")
    public void testBacklogTransactionShouldNotBeAbandon() throws Exception {
        TestHelper.dropTable(connection, "dbz6355");
        try {
            final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);

            connection.execute("CREATE TABLE dbz6355 (id numeric(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz6355");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, 60000L) // 1 Minute retention
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6355")
                    .build();

            // Insert one snapshot record.
            // Guarantees that we flush offsets.
            connection.execute("INSERT INTO dbz6355 (id,name) values (1, 'Gerald Jinx Mouse')");

            // Start connector and wait for streaming to begin
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Get only record
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6355");
            assertThat(tableRecords).hasSize(1);

            // Assert record state
            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Gerald Jinx Mouse");

            stopConnector();

            // Insert streaming record as in-progress transaction
            // Guarantees that we flush offsets.
            // It is expected that this will be discarded due to retention policy of 1 minute.
            try (OracleConnection otherConnection = TestHelper.testConnection()) {
                otherConnection.executeWithoutCommitting("INSERT INTO dbz6355 (id,name) values (2, 'Minnie Mouse')");

                LOGGER.info("Waiting {}ms for second change to age; should not be captured.", 70_000L);
                Thread.sleep(70_000L);

                // Restart the connector after downtime
                start(OracleConnector.class, config);
                assertConnectorIsRunning();

                waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

                // Get the number of fetching queries up to this point.
                final Long fetchingQueryCount = getStreamingMetric("FetchQueryCount");

                connection.execute("INSERT INTO dbz6355 (id,name) VALUES (3, 'Donald Duck')");

                // Fetch for a few mining iterations to guarantee that the abandonment process has fired
                Awaitility.waitAtMost(Duration.ofSeconds(60)).until(() -> {
                    return (fetchingQueryCount + 5L) <= (Long) getStreamingMetric("FetchQueryCount");
                });

                // Commit in progress transaction
                otherConnection.commit();

                Awaitility.await()
                        .atMost(Duration.ofSeconds(60))
                        .until(() -> logInterceptor.containsWarnMessage(" is being abandoned."));
            }

            // Get only record
            records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6355");
            assertThat(tableRecords).hasSize(1);

            // Assert record state
            after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("NAME")).isEqualTo("Donald Duck");

            // There should be no more records to consume.
            // The persisted state should contain the Thomas Jasper insert
            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6355");
        }
    }

    @Test
    @FixFor("DBZ-6439")
    public void shouldGetTableMetadataOnlyForCapturedTables() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                .with(OracleConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .build();

        LogInterceptor logInterceptor = new LogInterceptor(JdbcConnection.class);
        logInterceptor.setLoggerLevel(JdbcConnection.class, Level.DEBUG);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        stopConnector();

        assertThat(logInterceptor.containsMessage("1 table(s) will be scanned")).isTrue();
    }

    @Test
    @FixFor("DBZ-6499")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldRestartOracleJdbcConnectionAtMaxSessionThreshold() throws Exception {
        // In order to guarantee there are no log switches during this test, this test will preemptively
        // perform a transaction log switch before initiating the test.
        TestHelper.forceLogfileSwitch();

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.LOG_MINING_SESSION_MAX_MS, "30000")
                .with(OracleConnectorConfig.LOG_MINING_RESTART_CONNECTION, "true")
                .build();

        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
        logInterceptor.setLoggerLevel(AbstractLogMinerStreamingChangeEventSource.class, Level.DEBUG);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Session should trigger a restart after 30 seconds of streaming
        // After the restart has been triggered, it is safe to stop the connector
        Awaitility.await()
                .atMost(Duration.ofMinutes(2))
                .until(() -> logInterceptor.containsMessage("restarting Oracle JDBC connection"));

        // Give the connector a few seconds to restart the mining loop before stopping
        Thread.sleep(5000);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-6499")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies only to LogMiner")
    public void shouldRestartOracleJdbcConnectionUponLogSwitch() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.LOG_MINING_RESTART_CONNECTION, "true")
                .build();

        LogInterceptor logInterceptor = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
        logInterceptor.setLoggerLevel(AbstractLogMinerStreamingChangeEventSource.class, Level.DEBUG);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Wait cycle
        // 1. Waits for one mining loop, after which a Log Switch is forced.
        // 2. Once Log Switch is forced, wait for connector to detect and write log entry.
        // 3. Once connector has restarted the JDBC connection, wait loop exits.
        final AtomicBoolean completedOneMiningLoop = new AtomicBoolean();
        final AtomicBoolean logSwitchForced = new AtomicBoolean();
        Awaitility.await()
                .atMost(Duration.ofMinutes(2))
                .until(() -> {
                    if (!completedOneMiningLoop.get()) {
                        if (!logInterceptor.containsMessage("Oracle Session UGA")) {
                            return false;
                        }
                        else {
                            completedOneMiningLoop.set(true);
                        }
                    }
                    if (!logSwitchForced.get()) {
                        logSwitchForced.set(true);
                        TestHelper.forceLogfileSwitch();
                        return false;
                    }
                    return logInterceptor.containsMessage("restarting Oracle JDBC connection");
                });

        // Give the connector a few seconds to restart the mining loop before stopping
        Thread.sleep(5000);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-6528")
    public void shouldNotFailToStartWhenSignalDataCollectionNotDefinedWithinTableIncludeList() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz6528");
            TestHelper.dropTable(connection, "dbz6495");

            try (OracleConnection admin = TestHelper.adminConnection()) {
                if (TestHelper.isUsingPdb()) {
                    admin.setSessionToPdb(TestHelper.getDatabaseName());
                }
                TestHelper.dropTable(admin, "c##dbzuser.signals");
                admin.execute("CREATE TABLE c##dbzuser.signals (id varchar2(64), type varchar2(32), data varchar2(2048))");
                TestHelper.streamTable(admin, "c##dbzuser.signals");
            }

            connection.execute("CREATE TABLE dbz6528 (id numeric(9,0), data varchar2(50))");
            TestHelper.streamTable(connection, "dbz6528");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6528")
                    .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".C##DBZUSER.SIGNALS")
                    .with(OracleConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, "true")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz6528 (id,data) values (1, 'data')");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6528");
            assertThat(tableRecords).hasSize(1);

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6528");
            try (OracleConnection admin = TestHelper.adminConnection()) {
                if (TestHelper.isUsingPdb()) {
                    admin.setSessionToPdb(TestHelper.getDatabaseName());
                }
                TestHelper.dropTable(admin, "c##dbzuser.signals");
            }
        }
    }

    @Test
    @FixFor("DBZ-6650")
    public void shouldNotThrowConcurrentModificationExceptionWhenDispatchingSchemaChangeEvent() throws Exception {
        // The reporter's use case shows a log snippet where the table in question was not part of
        // the connector's schema history initially, so we'll build the test case around that.
        TestHelper.dropTable(connection, "dbz6650_snapshot");
        TestHelper.dropTable(connection, "dbz6650_stream");
        try {
            connection.execute("CREATE TABLE dbz6650_snapshot (id numeric(9,0), data varchar2(50), primary key(id))");
            connection.execute("INSERT INTO dbz6650_snapshot values (1, 'data')");
            TestHelper.streamTable(connection, "dbz6650_snapshot");

            connection.execute("CREATE TABLE dbz6650_stream (id numeric(9,0), data varchar2(50), primary key(id))");
            connection.execute("INSERT INTO dbz6650_stream values (1, 'data')");
            TestHelper.streamTable(connection, "dbz6650_stream");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6650_SNAPSHOT")
                    .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(OracleConnectorConfig.SKIPPED_OPERATIONS, "none") // explicitly needed
                    .with(OracleConnectorConfig.STORE_ONLY_CAPTURED_TABLES_DDL, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Expected: 1 create for known table and 1 insert.
            SourceRecords records = consumeRecordsByTopic(2);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6650_SNAPSHOT");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);

            List<SourceRecord> schemaRecords = records.recordsForTopic("server1");
            assertThat(schemaRecords).hasSize(1);

            // Now at this point its safe to stop the connector and reconfigure it so that it includes the stream
            // table that wasn't captured as part of the connector's schema history capture.
            stopConnector();

            config = config.edit()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6650_SNAPSHOT,DEBEZIUM\\.DBZ6650_STREAM")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertNoRecordsToConsume();

            connection.execute("TRUNCATE TABLE dbz6650_stream");
            connection.execute("INSERT INTO dbz6650_stream (id,data) values (2,'data')");

            // Expected: 1 truncate (DDL), 1 truncate (DML), and 1 insert
            records = consumeRecordsByTopic(3);

            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6650_STREAM");
            assertThat(tableRecords).hasSize(2);
            VerifyRecord.isValidTruncate(tableRecords.get(0));
            VerifyRecord.isValidInsert(tableRecords.get(1), "ID", 2);

            schemaRecords = records.recordsForTopic("server1");
            assertThat(schemaRecords).hasSize(1);

        }
        finally {
            TestHelper.dropTable(connection, "dbz6650_snapshot");
            TestHelper.dropTable(connection, "dbz6650_stream");
        }
    }

    @Test
    @FixFor("DBZ-6660")
    @SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER)
    public void shouldPauseAndWaitForDeviationCalculationIfBeforeMiningRange() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz6660");

            connection.execute("CREATE TABLE dbz6660 (id number(9,0), data varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz6660");

            final Long deviationMs = 10000L;

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6660")
                    .with(OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS, deviationMs.toString())
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MAX, "100")
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_DEFAULT, "100")
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MIN, "100")
                    .build();

            final LogInterceptor sourceLogging = new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            sourceLogging.setLoggerLevel(AbstractLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            final LogInterceptor processorLogging = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            processorLogging.setLoggerLevel(BufferedLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // This should be printed at the start of the streaming session while the back-fill is prepared.
            // Based on the time given, this could be printed several times.
            Awaitility.await().atMost(Duration.ofSeconds(60))
                    .until(() -> sourceLogging.containsMessage("outside of mining range, recalculating."));

            // Assert that every lag log entry is at least 10s behind due to deviation.
            try {
                final Pattern pattern = Pattern.compile("Lag: ([0-9]+)");
                final AtomicInteger id = new AtomicInteger(1);
                Awaitility.await()
                        .pollInterval(Duration.ofSeconds(1))
                        .atMost(Duration.ofSeconds(60)).until(() -> {
                            // Provide some dummy captured data periodically
                            connection.execute("INSERT INTO dbz6660 values (" + id.getAndIncrement() + ", 'data')");
                            final List<String> entries = processorLogging.getLogEntriesThatContainsMessage("Processed in ");
                            for (String entry : entries) {
                                final Matcher matcher = pattern.matcher(entry);
                                if (matcher.matches()) {
                                    assertThat(Long.valueOf(matcher.group(1))).isGreaterThan(deviationMs);
                                }
                            }
                            return false;
                        });
            }
            catch (ConditionTimeoutException e) {
                // ignored
            }

            // Assert connector did not fail
            assertConnectorIsRunning();

            // Just concerned that every iteration has lag greater than deviation.
            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6660");
        }
    }

    @Test
    @FixFor("DBZ-6660")
    @Ignore("Test can be flaky when using a brand new docker instance")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
    public void shouldUseEndScnIfDeviationProducesScnOutsideOfUndoRetention() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz6660");

            connection.execute("CREATE TABLE dbz6660 (id number(9,0), data varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz6660");

            // This is effectively 166666.667 minutes = 115.74 days
            // No Oracle instance should have undo space this large :)
            final Long deviationMs = 10000000000L;

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6660")
                    .with(OracleConnectorConfig.LOG_MINING_MAX_SCN_DEVIATION_MS, deviationMs.toString())
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MAX, "100")
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_DEFAULT, "100")
                    .with(OracleConnectorConfig.LOG_MINING_BATCH_SIZE_MIN, "100")
                    .build();

            final LogInterceptor sourceLogging = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            sourceLogging.setLoggerLevel(BufferedLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // This should be printed at the start of the streaming session while the back-fill is prepared.
            // Based on the time given, this could be printed several times.
            Awaitility.await().atMost(Duration.ofSeconds(60))
                    .until(() -> sourceLogging.containsMessage("outside undo space, using upperbounds"));

            // Just concerned that every iteration has lag greater than deviation.
            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6660");
        }
    }

    @Test
    @FixFor("DBZ-6677")
    public void shouldCaptureInvisibleColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz6677");
        try {
            connection.execute("CREATE TABLE dbz6677 (id number(9,0) primary key, data varchar2(50), data2 varchar2(50))");
            connection.execute("INSERT INTO dbz6677 values (1, 'Daffy', 'Daffy')");
            TestHelper.streamTable(connection, "dbz6677");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6677")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6677");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidRead(tableRecords.get(0), "ID", 1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(FieldName.AFTER);
            assertThat(after.get("DATA")).isEqualTo("Daffy");
            assertThat(after.get("DATA2")).isEqualTo("Daffy");

            connection.execute("ALTER TABLE dbz6677 modify data invisible");
            connection.execute("UPDATE dbz6677 set DATA2 = 'Donald' WHERE id = 1");

            records = consumeRecordsByTopic(1);

            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6677");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidUpdate(tableRecords.get(0), "ID", 1);

            after = ((Struct) tableRecords.get(0).value()).getStruct(FieldName.AFTER);
            assertThat(after.get("DATA")).isEqualTo("Daffy");
            assertThat(after.get("DATA2")).isEqualTo("Donald");

            connection.execute("ALTER TABLE dbz6677 modify data visible");
            connection.execute("INSERT INTO dbz6677 values (3, 'Hewy', 'Hewy')");

            records = consumeRecordsByTopic(1);

            tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6677");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 3);

            after = ((Struct) tableRecords.get(0).value()).getStruct(FieldName.AFTER);
            assertThat(after.get("DATA")).isEqualTo("Hewy");
            assertThat(after.get("DATA2")).isEqualTo("Hewy");
        }
        finally {
            TestHelper.dropTable(connection, "dbz6677");
        }
    }

    @Test
    @FixFor("DBZ-6975")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "LogMiner performs DML parsing")
    public void shouldHandleEscapedSingleQuotesInCharacterFields() throws Exception {
        TestHelper.dropTable(connection, "dbz6975");
        try {
            connection.execute("CREATE TABLE dbz6975 (c0 varchar2(50), c1 nvarchar2(50), c2 char(10), c3 nchar(10))");
            TestHelper.streamTable(connection, "dbz6975");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6975")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            // This is only an issue during LogMiner Streaming as we do not use the parser
            // during the snapshot phase.
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            String v = "UNISTR('\\0412') || 'a''b\\c'";
            connection.execute("INSERT INTO dbz6975 values (" + String.join(",", v, v, v, v) + ")");
            String u = "UNISTR('\\041D') || 'bc''d'";
            connection.execute("UPDATE dbz6975 set c0=" + u + ", c1=" + u + ",c2=" + u + ",c3=" + u);
            connection.execute("DELETE FROM dbz6975");

            SourceRecords records = consumeRecordsByTopic(3);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6975");
            assertThat(tableRecords).hasSize(3);

            // NOTE:
            // The following assertions show that CHAR fields with multi-byte characters are short
            // 1 character, this is because Oracle interprets the two bytes as two separate bytes
            // due to the field being CHAR, thus the final output is short by 1 character padding.
            // As seen below with NCHAR columns, the padding properly aligns to the column width
            // as expected. In short, don't insert multibyte data into CHAR fields and expect the
            // database will properly account for the padding!!! :)

            // Insert
            SourceRecord record = tableRecords.get(0);
            VerifyRecord.isValidInsert(record);
            assertThat(getAfter(record).get("C0")).isEqualTo("Вa'b\\c");
            assertThat(getAfter(record).get("C1")).isEqualTo("Вa'b\\c");
            assertThat(getAfter(record).get("C2")).isEqualTo("Вa'b\\c   "); // CHAR is padded
            assertThat(getAfter(record).get("C3")).isEqualTo("Вa'b\\c    "); // NCHAR is padded

            // Update
            record = tableRecords.get(1);
            VerifyRecord.isValidUpdate(record);
            assertThat(getAfter(record).get("C0")).isEqualTo("Нbc'd");
            assertThat(getAfter(record).get("C1")).isEqualTo("Нbc'd");
            assertThat(getAfter(record).get("C2")).isEqualTo("Нbc'd    "); // CHAR is padded
            assertThat(getAfter(record).get("C3")).isEqualTo("Нbc'd     "); // NCHAR is padded

            // Delete
            record = tableRecords.get(2);
            VerifyRecord.isValidDelete(record);
            assertThat(getBefore(record).get("C0")).isEqualTo("Нbc'd");
            assertThat(getBefore(record).get("C1")).isEqualTo("Нbc'd");
            assertThat(getBefore(record).get("C2")).isEqualTo("Нbc'd    "); // CHAR is padded
            assertThat(getBefore(record).get("C3")).isEqualTo("Нbc'd     "); // NCHAR is padded

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz6975");
        }
    }

    @Test
    @FixFor({ "DBZ-4332", "DBZ-7823" })
    public void shouldCaptureRowIdForDataChanges() throws Exception {
        TestHelper.dropTable(connection, "dbz4332");
        try {
            connection.execute("CREATE TABLE dbz4332 (id number(9,0), data varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4332");

            connection.execute("INSERT INTO dbz4332 VALUES (1, 'snapshot')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4332")
                    .with(OracleConnectorConfig.TOMBSTONES_ON_DELETE, "false")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz4332 VALUES (2, 'streaming')");
            connection.execute("UPDATE dbz4332 set data = 'update'");
            connection.execute("DELETE FROM dbz4332");

            final SourceRecords sourceRecords = consumeRecordsByTopic(6);
            final List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4332");
            assertThat(records).hasSize(6);

            for (int i = 0; i < records.size(); i++) {
                final Struct source = ((Struct) records.get(i).value()).getStruct("source");
                if (i == 0) {
                    // Snapshots do not capture row ids
                    assertThat(source.get("row_id")).isNull();
                }
                else {
                    assertThat(source.get("row_id")).isNotNull();
                }
            }

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4332");
        }
    }

    @Test
    @FixFor("DBZ-7831")
    public void shouldStreamChangesForTableWithSingleQuote() throws Exception {
        TestHelper.dropTable(connection, "\"debezium_test'\"");
        try {
            connection.execute("CREATE TABLE \"debezium_test'\"\n" +
                    "(\n" +
                    "    id NUMBER(10),\n" +
                    "    first_name VARCHAR2(50),\n" +
                    "    last_name VARCHAR2(50),\n" +
                    "    PRIMARY KEY(ID)\n" +
                    ")");
            connection.execute("INSERT INTO \"debezium_test'\" (id,first_name,last_name) values (1,'Andy','Griffith')");
            TestHelper.streamTable(connection, "\"debezium_test'\"");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.debezium_test'")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO \"debezium_test'\" (id,first_name,last_name) values (2,'Elmer','Fudd')");

            final SourceRecords sourceRecords = consumeRecordsByTopic(2);
            final List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.debezium_test_");
            assertThat(records).hasSize(2);
        }
        finally {
            TestHelper.dropTable(connection, "\"debezium_test'\"");
        }
    }

    @Test
    @FixFor("DBZ-8577")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Specific to LogMiner")
    public void shouldRestoreConnectionStateWhenConnectionIsRestartedOnMiningRestartConnectToPdb() throws Exception {
        TestHelper.dropTable(connection, "dbz8577");
        try {
            connection.execute("CREATE TABLE dbz8577 (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8577");

            Configuration tempConfig = TestHelper.defaultConfig().build();

            final Configuration.Builder builder = TestHelper.defaultConfig();
            if (!Strings.isNullOrEmpty(tempConfig.getString(OracleConnectorConfig.PDB_NAME))) {
                // For this use case, both PDB and DBNAME should refer to PDB
                // This is an unconventional configuration, but permissible.
                builder.with(OracleConnectorConfig.DATABASE_NAME, tempConfig.getString(OracleConnectorConfig.PDB_NAME));
            }
            builder.with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8577")
                    .with(OracleConnectorConfig.LOG_MINING_RESTART_CONNECTION, "true");

            start(OracleConnector.class, builder.build());
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Wait for CURRENT_SCN to be seen by the connector
            // Then force a log switch
            // Then wait for new CURRENT_SCN to be seen by the connector
            waitForCurrentScnToHaveBeenSeenByConnector();
            TestHelper.forceLogfileSwitch();
            waitForCurrentScnToHaveBeenSeenByConnector();

            // Insert a row to act as a marker
            connection.execute("INSERT INTO dbz8577 (id,data) values (1,'test')");

            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ8577")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz8577");
        }
    }

    @Test
    @FixFor("DBZ-8577")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Specific to LogMiner")
    public void shouldRestoreConnectionStateWhenConnectionIsRestartedOnMiningRestart() throws Exception {
        TestHelper.dropTable(connection, "dbz8577");
        try {
            connection.execute("CREATE TABLE dbz8577 (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8577");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8577")
                    .with(OracleConnectorConfig.LOG_MINING_RESTART_CONNECTION, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Wait for CURRENT_SCN to be seen by the connector
            // Then force a log switch
            // Then wait for new CURRENT_SCN to be seen by the connector
            waitForCurrentScnToHaveBeenSeenByConnector();
            TestHelper.forceLogfileSwitch();
            waitForCurrentScnToHaveBeenSeenByConnector();

            // Insert a row to act as a marker
            connection.execute("INSERT INTO dbz8577 (id,data) values (1,'test')");

            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ8577")).hasSize(1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz8577");
        }
    }

    @Test
    @FixFor("DBZ-8740")
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER, reason = "Applies to LogMiner")
    public void shouldPopulateCommitScnAndTimestampInSourceInfoBlock() throws Exception {
        TestHelper.dropTable(connection, "dbz8740");
        try {
            connection.execute("CREATE TABLE dbz8740 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8740");

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8740")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Insert, wait 2 seconds, and insert to guarantee differing SCN values
            connection.execute("INSERT INTO dbz8740 (id,data) values (1,'Test1')");
            Thread.sleep(2000);
            connection.execute("INSERT INTO dbz8740 (id,data) values (2,'Test2')");

            final SourceRecords records = consumeRecordsByTopic(2);
            final List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ8740");
            assertThat(tableRecords).hasSize(2);

            // Verify that the source info fields are populated

            Struct after1 = ((Struct) tableRecords.get(0).value()).getStruct(FieldName.AFTER);
            assertThat(after1.get("ID")).isEqualTo(1);
            assertThat(after1.get("DATA")).isEqualTo("Test1");

            Struct source1 = ((Struct) tableRecords.get(0).value()).getStruct(FieldName.SOURCE);
            assertThat(source1.get(SourceInfo.COMMIT_SCN_KEY)).isNotNull();
            assertThat(source1.get(SourceInfo.COMMIT_TIMESTAMP_KEY)).isNotNull();
            assertThat(source1.get(SourceInfo.START_SCN_KEY)).isNotNull();
            assertThat(source1.get(SourceInfo.START_TIMESTAMP_KEY)).isNotNull();

            Struct after2 = ((Struct) tableRecords.get(1).value()).getStruct(FieldName.AFTER);
            assertThat(after2.get("ID")).isEqualTo(2);
            assertThat(after2.get("DATA")).isEqualTo("Test2");

            Struct source2 = ((Struct) tableRecords.get(1).value()).getStruct(FieldName.SOURCE);
            assertThat(source2.get(SourceInfo.COMMIT_SCN_KEY)).isNotNull();
            assertThat(source2.get(SourceInfo.COMMIT_TIMESTAMP_KEY)).isNotNull();
            assertThat(source2.get(SourceInfo.START_SCN_KEY)).isNotNull();
            assertThat(source2.get(SourceInfo.START_TIMESTAMP_KEY)).isNotNull();

            // Test that first transaction values are prior to second transaction values

            final Scn startScn1 = Scn.valueOf(source1.getString(SourceInfo.START_SCN_KEY));
            final Scn startScn2 = Scn.valueOf(source2.getString(SourceInfo.START_SCN_KEY));
            assertThat(startScn1.compareTo(startScn2) < 0).isTrue();

            final Instant startTime1 = Instant.ofEpochMilli(source1.getInt64(SourceInfo.START_TIMESTAMP_KEY));
            final Instant startTime2 = Instant.ofEpochMilli(source2.getInt64(SourceInfo.START_TIMESTAMP_KEY));
            assertThat(startTime1.isBefore(startTime2)).isTrue();

            final Scn commitScn1 = Scn.valueOf(source1.getString(SourceInfo.COMMIT_SCN_KEY));
            final Scn commitScn2 = Scn.valueOf(source2.getString(SourceInfo.COMMIT_SCN_KEY));
            assertThat(commitScn1.compareTo(commitScn2) < 0).isTrue();

            final Instant commitTime1 = Instant.ofEpochMilli(source1.getInt64(SourceInfo.COMMIT_TIMESTAMP_KEY));
            final Instant commitTime2 = Instant.ofEpochMilli(source2.getInt64(SourceInfo.COMMIT_TIMESTAMP_KEY));
            assertThat(commitTime1.isBefore(commitTime2)).isTrue();

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8740");
        }
    }

    @Test
    @FixFor("DBZ-9132")
    public void shouldStreamMultibyteCharacterSequenceWithEmbeddedSqlConcatenationSequence() throws Exception {
        TestHelper.dropTable(connection, "dbz9132");
        try {
            connection.execute("CREATE TABLE dbz9132 (id numeric(9,0) primary key, data nvarchar2(500))");
            TestHelper.streamTable(connection, "dbz9132");

            connection.execute("INSERT INTO dbz9132 values (1, '中国||武汉')");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9132")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9132 values (2, '中国||武汉')");

            final List<SourceRecord> records = consumeRecordsByTopic(2).recordsForTopic("server1.DEBEZIUM.DBZ9132");
            assertThat(records).hasSize(2);

            for (SourceRecord record : records) {
                Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("DATA")).as("Record - " + record).isEqualTo("中国||武汉");
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz9132");
        }
    }

    @Test
    @Ignore("This test requires manual execution of RMAN steps, so it cannot be automated")
    @FixFor("DBZ-9416")
    public void shouldNotFailWhenLogIsNoLongerAvailable() throws Exception {
        // Before manually running this test, login to Oracle container using
        // docker exec -it -e ORACLE_SID=ORCLCDB <container-name> rman
        //
        // At the rman property, type "connect target" to connect to the database.
        // When prompted in the test, run "delete archivelog all;" and confirm by typing "YES".

        TestHelper.dropTable(connection, "dbztest");
        try {
            connection.execute("CREATE TABLE dbztest (id numeric(9,0) primary key, data varchar2(50))");
            connection.execute("INSERT INTO dbztest values (1, 'snapshot')");
            TestHelper.streamTable(connection, "dbztest");

            final LogInterceptor interceptor = new LogInterceptor(OracleConnectorIT.class);

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZTEST")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            consumeRecordsByTopic(1);

            stopConnector();

            // This makes sure that no online redo log may still have reference to the SCN that is
            // being forced rolled into the archive.
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            // This is where running "delete archivelog all;" should be executed
            System.out.println("Use RMAN to run deletion of all archive logs now.");
            Thread.sleep(15000);

            // Restarts the connector, and should report an error about cannot read change stream
            assertThatThrownBy(() -> {
                start(OracleConnector.class, config);
                assertConnectorIsRunning();
            }).isInstanceOf(ComparisonFailure.class);

            // Assert error thrown
            assertThat(interceptor.containsErrorMessage("The connector is trying to read change stream starting at")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbztest");
        }
    }

    private void waitForCurrentScnToHaveBeenSeenByConnector() throws SQLException {
        try (OracleConnection admin = TestHelper.adminConnection(true)) {
            final Scn scn = admin.getCurrentScn();
            Awaitility.await()
                    .atMost(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS)
                    .until(() -> {
                        final BigInteger scnValue = getStreamingMetric("CurrentScn");
                        if (scnValue == null) {
                            return false;
                        }
                        return new Scn(scnValue).compareTo(scn) > 0;
                    });
        }
    }

    private Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private Struct getBefore(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldValidateGuardrailLimitsExceedsMaximumTables() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor(CommonConnectorConfig.class);

        TestHelper.dropTable(connection, "debezium.customer2");
        try {
            String ddl = "create table debezium.customer2 (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar2(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")";

            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.customer2");

            connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            // Configure with guardrail limit of 1 table (less than 2 that connector is capturing)
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER.*")
                    .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 1)
                    .build();

            // The connector should continue to run even after exceeding the guardrail limit
            LOGGER.info("Attempting to start connector with guardrail limit exceeded, expect a warning");
            start(OracleConnector.class, config, (success, msg, error) -> {
                assertThat(success).isTrue();
                assertThat(error).isNull();
            });
            assertConnectorIsRunning();
            assertThat(logInterceptor.containsWarnMessage("Guardrail limit exceeded")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "debezium.customer2");
        }
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldValidateGuardrailLimitsExceedsMaximumTablesAndFailConnector() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");

        try {
            String ddl = "create table debezium.customer2 (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar2(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")";

            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.customer2");

            connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            // Configure with guardrail limit of 1 table (less than 2 that connector is capturing)
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER.*")
                    .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 1)
                    .with(CommonConnectorConfig.GUARDRAIL_TABLES_LIMIT_ACTION, "fail")
                    .build();

            // The connector should fail to start due to exceeding the guardrail limit
            LOGGER.info("Attempting to start connector with guardrail limit exceeded, expect an error");
            start(OracleConnector.class, config, (success, msg, error) -> {
                assertThat(success).isFalse();
                assertThat(error).isNotNull();
                assertThat(error.getMessage()).contains("Guardrail limit exceeded");
            });
            assertConnectorNotRunning();
        }
        finally {
            TestHelper.dropTable(connection, "debezium.customer2");
        }
    }

    @Test
    @FixFor("DBZ-9427")
    public void shouldStartSuccessfullyWithinGuardrailLimits() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer2");

        try {
            String ddl = "create table debezium.customer2 (" +
                    "  id numeric(9,0) not null, " +
                    "  name varchar2(1000), " +
                    "  score decimal(6, 2), " +
                    "  registered timestamp, " +
                    "  primary key (id)" +
                    ")";

            connection.execute(ddl);
            TestHelper.streamTable(connection, "debezium.customer2");

            connection.execute("INSERT INTO debezium.customer2 VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            // Configure with guardrail limit of 10 tables
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER.*")
                    .with(CommonConnectorConfig.GUARDRAIL_TABLES_MAX, 10)
                    .with(CommonConnectorConfig.GUARDRAIL_TABLES_LIMIT_ACTION, "fail")
                    .build();

            // The connector should start successfully
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("INSERT INTO debezium.customer2 VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            // Consume all records to ensure the connector is working
            SourceRecords records = consumeRecordsByTopic(2);
            assertThat(records).isNotNull();
            assertThat(records.topics()).hasSize(2);

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "debezium.customer2");
        }
    }

    @Test
    @FixFor("DBZ-9497")
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.OLR, reason = "OLR does not populate this field")
    public void shouldSetCommitScnInSourceInformationBlock() throws Exception {
        TestHelper.dropTable(connection, "dbz9497");
        try {
            connection.execute("CREATE TABLE dbz9497 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9497");

            // Configure with guardrail limit of 10 tables
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9497")
                    .build();

            // The connector should start successfully
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9497 values (1, 'test')");

            // Consume all records to ensure the connector is working
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records).isNotNull();
            assertThat(records.topics()).hasSize(1);

            final List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ9497");
            final Struct source = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(source.get("commit_scn")).isNotNull();

            stopConnector();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9497");
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium Oracle connector.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorFilterIT extends AbstractConnectorTest {

    private static OracleConnection connection;
    private static OracleConnection adminConnection;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        adminConnection = TestHelper.adminConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        TestHelper.dropTable(connection, "debezium.table1");
        TestHelper.dropTable(connection, "debezium.table2");
        TestHelper.dropTable(connection, "debezium.table3");

        try {
            adminConnection.execute("DROP USER debezium2 CASCADE");
        }
        catch (SQLException ignored) {
        }

        adminConnection.execute(
                "CREATE USER debezium2 IDENTIFIED BY dbz",
                "GRANT CONNECT TO debezium2",
                "GRANT CREATE SESSION TO debezium2",
                "GRANT CREATE TABLE TO debezium2",
                "GRANT CREATE SEQUENCE TO debezium2",
                "ALTER USER debezium2 QUOTA 100M ON users",
                "CREATE TABLE debezium2.table2 (id NUMERIC(9,0) NOT NULL, name VARCHAR2(1000), PRIMARY KEY (id))",
                "CREATE TABLE debezium2.nopk (id NUMERIC(9,0) NOT NULL)",
                "GRANT ALL PRIVILEGES ON debezium2.table2 TO debezium",
                "GRANT SELECT ON debezium2.table2 TO " + TestHelper.getConnectorUserName(),
                "GRANT SELECT ON debezium2.nopk TO " + TestHelper.getConnectorUserName(),
                "ALTER TABLE debezium2.table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        String ddl = "CREATE TABLE debezium.table1 (" +
                "  id NUMERIC(9,0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table1 TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.table1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        ddl = "CREATE TABLE debezium.table2 (" +
                "  id NUMERIC(9,0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table2 TO  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Log Miner does not support DDL during streaming")
    public void shouldApplyTableWhitelistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                        "DEBEZIUM2\\.TABLE2,DEBEZIUM\\.TABLE1,DEBEZIUM\\.TABLE3")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "CREATE TABLE debezium.table3 (" +
                "  id NUMERIC(9, 0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 TO c##xstrm");

        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
        after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Log Miner does not support DDL during streaming")
    public void shouldApplyTableIncludeListConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        OracleConnectorConfig.TABLE_INCLUDE_LIST,
                        "DEBEZIUM2\\.TABLE2,DEBEZIUM\\.TABLE1,DEBEZIUM\\.TABLE3")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "CREATE TABLE debezium.table3 (" +
                "  id NUMERIC(9, 0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.table3 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
        after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Log Miner does not support DDL during streaming")
    public void shouldApplyTableBlacklistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        OracleConnectorConfig.TABLE_BLACKLIST,
                        "DEBEZIUM\\.TABLE2,DEBEZIUM\\.CUSTOMER.*")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "CREATE TABLE debezium.table3 (" +
                "  id NUMERIC(9,0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 TO  " + TestHelper.getConnectorUserName());

        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
        after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "Log Miner does not support DDL during streaming")
    public void shouldApplyTableExcludeListConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        OracleConnectorConfig.TABLE_EXCLUDE_LIST,
                        "DEBEZIUM\\.TABLE2,DEBEZIUM\\.CUSTOMER.*")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "CREATE TABLE debezium.table3 (" +
                "  id NUMERIC(9,0) NOT NULL, " +
                "  name VARCHAR2(1000), " +
                "  PRIMARY KEY (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 TO  " + TestHelper.getConnectorUserName());

        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
        after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }

    @Test
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER)
    public void shouldTakeTimeDifference() throws Exception {
        Testing.Print.enable();
        String stmt = "select current_timestamp from dual";
        try (Connection conn = connection.connection(true);
                PreparedStatement ps = conn.prepareStatement(stmt);
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            java.sql.Timestamp ts = rs.getTimestamp(1);
            Instant fromDb = ts.toInstant();
            Instant now = Instant.now();
            long diff = Duration.between(fromDb, now).toMillis();
            Testing.print("diff: " + diff);
        }
    }
}

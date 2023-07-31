/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
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
        if (adminConnection != null) {
            adminConnection.close();
        }
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.table1");
            TestHelper.dropTable(connection, "debezium.table2");
            TestHelper.dropTable(connection, "debezium.table3");
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
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        TestHelper.dropTable(adminConnection, "debezium2.table2");
        TestHelper.dropTable(adminConnection, "debezium2.nopk");
        adminConnection.execute("DROP USER debezium2");
    }

    @Test
    public void shouldApplyTableIncludeListConfiguration() throws Exception {
        shouldApplyTableInclusionConfiguration();
    }

    @Test
    public void shouldApplyTableExcludeListConfiguration() throws Exception {
        shouldApplyTableExclusionsConfiguration();
    }

    @Test
    @FixFor("DBZ-3009")
    public void shouldApplySchemaAndTableIncludeListConfiguration() throws Exception {
        shouldApplySchemaAndTableInclusionConfiguration();
    }

    @Test
    @FixFor("DBZ-3009")
    public void shouldApplySchemaAndTableExcludeListConfiguration() throws Exception {
        shouldApplySchemaAndTableExclusionsConfiguration();
    }

    @Test
    @FixFor({ "DBZ-3167", "DBZ-3219" })
    public void shouldApplyColumnIncludeListConfiguration() throws Exception {
        TestHelper.dropTable(connection, "table4");
        try {
            String ddl = "CREATE TABLE debezium.table4 (" +
                    "  id NUMERIC(9,0) NOT NULL, " +
                    "  name VARCHAR2(1000), " +
                    "  birth_date date, " +
                    "  PRIMARY KEY (id)" +
                    ")";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table4 TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.table4 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO debezium.table4 VALUES (1, 'Text-1', TO_DATE('1990-01-01', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLE4")
                    .with(OracleConnectorConfig.COLUMN_INCLUDE_LIST, "DEBEZIUM\\.TABLE4\\.ID,DEBEZIUM\\.TABLE4\\.NAME")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Fetch the snapshot records
            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE4");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidRead(testTableRecords.get(0), "ID", 1);
            Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Text-1");
            assertThat(after.schema().field("BIRTH_DATE")).isNull();

            // Start streaming & wait for it
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.table4 VALUES (2, 'Text-2', TO_DATE('1990-12-31', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            records = consumeRecordsByTopic(1);

            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE4");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 2);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("NAME")).isEqualTo("Text-2");
            assertThat(after.schema().field("BIRTH_DATE")).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "table4");
        }
    }

    @Test
    @FixFor({ "DBZ-3167", "DBZ-3219" })
    public void shouldApplyColumnExcludeListConfiguration() throws Exception {
        TestHelper.dropTable(connection, "table4");
        try {
            String ddl = "CREATE TABLE debezium.table4 (" +
                    "  id NUMERIC(9,0) NOT NULL, " +
                    "  name VARCHAR2(1000), " +
                    "  birth_date date, " +
                    "  PRIMARY KEY (id)" +
                    ")";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table4 TO " + TestHelper.getConnectorUserName());
            connection.execute("ALTER TABLE debezium.table4 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

            connection.execute("INSERT INTO debezium.table4 VALUES (1, 'Text-1', TO_DATE('1990-01-01', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TABLE4")
                    .with(OracleConnectorConfig.COLUMN_EXCLUDE_LIST, "DEBEZIUM\\.TABLE4\\.BIRTH_DATE")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Fetch the snapshot records
            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE4");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidRead(testTableRecords.get(0), "ID", 1);
            Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Text-1");
            assertThat(after.schema().field("BIRTH_DATE")).isNull();

            // Start streaming & wait for it
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO debezium.table4 VALUES (2, 'Text-2', TO_DATE('1990-12-31', 'yyyy-mm-dd'))");
            connection.execute("COMMIT");

            records = consumeRecordsByTopic(1);

            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE4");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 2);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("NAME")).isEqualTo("Text-2");
            assertThat(after.schema().field("BIRTH_DATE")).isNull();
        }
        finally {
            TestHelper.dropTable(connection, "table4");
        }
    }

    private void shouldApplyTableInclusionConfiguration() throws Exception {
        Field option = OracleConnectorConfig.TABLE_INCLUDE_LIST;
        boolean includeDdlChanges = true;
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            // LogMiner currently does not support DDL changes during streaming phase
            includeDdlChanges = false;
        }

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                .with(option, "DEBEZIUM2\\.TABLE2,DEBEZIUM\\.TABLE1,DEBEZIUM\\.TABLE3")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        if (includeDdlChanges) {
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
        }

        SourceRecords records = consumeRecordsByTopic(includeDdlChanges ? 2 : 1);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        if (includeDdlChanges) {
            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("NAME")).isEqualTo("Text-3");
        }
    }

    private void shouldApplySchemaAndTableInclusionConfiguration() throws Exception {
        Field option = OracleConnectorConfig.TABLE_INCLUDE_LIST;
        boolean includeDdlChanges = true;
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            // LogMiner currently does not support DDL changes during streaming phase
            includeDdlChanges = false;
        }

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM,DEBEZIUM2")
                .with(option, "DEBEZIUM2\\.TABLE2,DEBEZIUM\\.TABLE1,DEBEZIUM\\.TABLE3")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("INSERT INTO debezium2.table2 VALUES (1, 'Text2-1')");
        connection.execute("COMMIT");

        if (includeDdlChanges) {
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
        }

        // Xstream binds a single schema to be captured, so changes in debezium2.table2 aren't captured
        // LogMiner supports schemas based on schema configurations, so debezium2.table2 is captured,
        // however due to includeDdlChanges = false, debezium.table3 isn't performed.
        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM2.TABLE2");
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.XSTREAM) ||
                TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.OLR)) {
            assertThat(testTableRecords).isNull();
        }
        else {
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Text2-1");
        }

        if (includeDdlChanges) {
            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("NAME")).isEqualTo("Text-3");
        }
    }

    private void shouldApplyTableExclusionsConfiguration() throws Exception {
        Field option = OracleConnectorConfig.TABLE_EXCLUDE_LIST;
        boolean includeDdlChanges = true;
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            // LogMiner currently does not support DDL changes during streaming phase
            includeDdlChanges = false;
        }

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                .with(option, "DEBEZIUM\\.TABLE2,DEBEZIUM\\.CUSTOMER.*")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        if (includeDdlChanges) {
            String ddl = "CREATE TABLE debezium.table3 (" +
                    "  id NUMERIC(9,0) NOT NULL, " +
                    "  name VARCHAR2(1000), " +
                    "  PRIMARY KEY (id)" +
                    ")";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table3 TO  " + TestHelper.getConnectorUserName());

            connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
            connection.execute("COMMIT");
        }

        SourceRecords records = consumeRecordsByTopic(includeDdlChanges ? 2 : 1);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 1);
        Struct after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        if (includeDdlChanges) {
            testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
            assertThat(testTableRecords).hasSize(1);

            VerifyRecord.isValidInsert(testTableRecords.get(0), "ID", 3);
            after = (Struct) ((Struct) testTableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(3);
            assertThat(after.get("NAME")).isEqualTo("Text-3");
        }
    }

    private void shouldApplySchemaAndTableExclusionsConfiguration() throws Exception {
        Field option = OracleConnectorConfig.TABLE_EXCLUDE_LIST;
        boolean includeDdlChanges = true;
        boolean isLogMiner = false;
        boolean isOlr = false;
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            // LogMiner currently does not support DDL changes during streaming phase
            includeDdlChanges = false;
            isLogMiner = true;
        }
        else if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.OLR)) {
            isOlr = true;
        }

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SCHEMA_EXCLUDE_LIST, "DEBEZIUM,SYS")
                .with(option, "DEBEZIUM\\.TABLE2,DEBEZIUM\\.CUSTOMER.*,DEBEZIUM2\\.NOPK")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("INSERT INTO debezium2.table2 VALUES (1, 'Text2-1')");
        connection.execute("COMMIT");

        if (includeDdlChanges) {
            String ddl = "CREATE TABLE debezium.table3 (" +
                    "  id NUMERIC(9,0) NOT NULL, " +
                    "  name VARCHAR2(1000), " +
                    "  PRIMARY KEY (id)" +
                    ")";

            connection.execute(ddl);
            connection.execute("GRANT SELECT ON debezium.table3 TO  " + TestHelper.getConnectorUserName());

            connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
            connection.execute("COMMIT");
        }

        // Xstream binds a single schema to be captured, so changes in debezium2.table2 aren't captured
        // LogMiner supports schemas based on schema configurations, so debezium2.table2 is captured,
        // however due to includeDdlChanges = false, debezium.table3 isn't performed.
        if (isLogMiner) {
            SourceRecords records = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE1");
            assertThat(tableRecords).isNull();

            tableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE2");
            assertThat(tableRecords).isNull();

            tableRecords = records.recordsForTopic("server1.DEBEZIUM2.TABLE2");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
            Struct after = (Struct) ((Struct) tableRecords.get(0).value()).get("after");
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Text2-1");

            if (includeDdlChanges) {
                tableRecords = records.recordsForTopic("server1.DEBEZIUM.TABLE3");
                assertThat(tableRecords).isNull();
            }
        }
        else {
            assertNoRecordsToConsume();
        }
    }

    @Test
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

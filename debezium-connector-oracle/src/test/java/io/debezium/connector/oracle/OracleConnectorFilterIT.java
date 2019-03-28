/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
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
        catch (SQLException e) {
        }

        adminConnection.execute(
                "CREATE USER debezium2 IDENTIFIED BY dbz",
                "GRANT CONNECT TO debezium2",
                "GRANT CREATE SESSION TO debezium2",
                "GRANT CREATE TABLE TO debezium2",
                "GRANT CREATE SEQUENCE TO debezium2",
                "ALTER USER debezium2 QUOTA 100M ON users",
                "create table debezium2.table2 (id numeric(9,0) not null,name varchar2(1000),primary key (id))",
                "GRANT ALL PRIVILEGES ON debezium2.table2 to debezium",
                "GRANT SELECT ON debezium2.table2 to " + TestHelper.CONNECTOR_USER,
                "ALTER TABLE debezium2.table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
            );
        String ddl = "create table debezium.table1 (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table1 to " + TestHelper.CONNECTOR_USER);
        connection.execute("ALTER TABLE debezium.table1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        ddl = "create table debezium.table2 (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table2 to  " + TestHelper.CONNECTOR_USER);
        connection.execute("ALTER TABLE debezium.table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    public void shouldApplyWhitelistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                        "ORCLPDB1\\.DEBEZIUM2\\.TABLE2,ORCLPDB1\\.DEBEZIUM\\.TABLE1,ORCLPDB1\\.DEBEZIUM\\.TABLE3")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "create table debezium.table3 (" +
                "  id numeric(9, 0) not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 to c##xstrm");

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
    public void shouldApplyBlacklistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                        "ORCLPDB1\\.DEBEZIUM\\.TABLE2,ORCLPDB1\\.DEBEZIUM\\.CUSTOMER.*")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "create table debezium.table3 (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 to  " + TestHelper.CONNECTOR_USER);

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
}

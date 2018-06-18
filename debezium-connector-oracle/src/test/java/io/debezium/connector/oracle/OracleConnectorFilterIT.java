/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
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

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        TestHelper.dropTable(connection, "debezium.table1");
        TestHelper.dropTable(connection, "debezium.table2");
        TestHelper.dropTable(connection, "debezium.table3");

        String ddl = "create table debezium.table1 (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table1 to c##xstrmadmin");
        connection.execute("ALTER TABLE debezium.table1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        ddl = "create table debezium.table2 (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table2 to c##xstrmadmin");
        connection.execute("ALTER TABLE debezium.table2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    public void shouldApplyWhitelistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.TABLE_WHITELIST,
                        "ORCLPDB1\\.DEBEZIUM\\.TABLE1,ORCLPDB1\\.DEBEZIUM\\.TABLE3")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "create table debezium.table3 (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 to c##xstrmadmin");

        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(3));
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }

    @Test
    public void shouldApplyBlacklistConfiguration() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(
                        RelationalDatabaseConnectorConfig.TABLE_BLACKLIST,
                        "ORCLPDB1\\.DEBEZIUM\\.TABLE2,ORCLPDB1\\.DEBEZIUM\\.CUSTOMER.*")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        connection.execute("INSERT INTO debezium.table1 VALUES (1, 'Text-1')");
        connection.execute("INSERT INTO debezium.table2 VALUES (2, 'Text-2')");
        connection.execute("COMMIT");

        String ddl = "create table debezium.table3 (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.table3 to c##xstrmadmin");

        connection.execute("INSERT INTO debezium.table3 VALUES (3, 'Text-3')");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(2);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE1");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Text-1");

        testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE2");
        assertThat(testTableRecords).isNull();

        testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.TABLE3");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(3));
        assertThat(after.get("NAME")).isEqualTo("Text-3");
    }
}

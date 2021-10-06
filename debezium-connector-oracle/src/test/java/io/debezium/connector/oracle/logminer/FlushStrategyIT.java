/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy.LOGMNR_FLUSH_TABLE;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnDatabaseOptionRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Flush strategy only applies to LogMiner implementation")
public class FlushStrategyIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();
    @Rule
    public final TestRule skipOptionRule = new SkipTestDependingOnDatabaseOptionRule();

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
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-4118")
    public void shouldOnlyMaintainOneRowInFlushStrategyTable() throws Exception {
        try {
            TestHelper.dropTable(connection, "dbz4118");
            connection.execute("CREATE TABLE dbz4118 (id numeric(9,0), data varchar2(50), primary key(id))");
            TestHelper.streamTable(connection, "dbz4118");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4118")
                    .build();

            // Start connector as if its brand new, no flush table exists
            dropFlushTable();

            // Start connector
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Use a single insert as a marker entry to know when its safe to test flush strategy table
            connection.execute("INSERT INTO dbz4118 (id,data) values (1,'Test')");
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4118")).hasSize(1);

            // Verify only one row after a record is captured in streaming loop
            assertFlushTableHasExactlyOneRow();

            // Restart the connector to simulate an existing connector
            stopConnector();

            // Insert a second row into flush table
            insertFlushTable("12345");

            LogInterceptor logInterceptor = new LogInterceptor();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify that the connector logged multiple rows detected and fixed
            assertThat(logInterceptor.containsWarnMessage("DBZ-4118: The flush table, " + LOGMNR_FLUSH_TABLE + ", has multiple rows")).isTrue();

            // Verify that no additional rows get inserted on restart
            assertFlushTableHasExactlyOneRow();

            // Use a single insert as a marker entry to know when its safe to test flush strategy table
            connection.execute("INSERT INTO dbz4118 (id,data) values (2,'Test')");
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4118")).hasSize(1);

            // Verify only one row after a record is captured in streaming loop
            assertFlushTableHasExactlyOneRow();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4118");
        }
    }

    private void assertFlushTableHasExactlyOneRow() throws SQLException {
        try (OracleConnection conn = TestHelper.defaultConnection()) {
            conn.resetSessionToCdb();
            assertThat(conn.getRowCount(getFlushTableName())).isEqualTo(1L);
        }
    }

    private void dropFlushTable() throws SQLException {
        try (OracleConnection admin = TestHelper.adminConnection()) {
            admin.resetSessionToCdb();
            TestHelper.dropTable(admin, getFlushTableName());
        }
    }

    private void insertFlushTable(String scnValue) throws SQLException {
        try (OracleConnection conn = TestHelper.defaultConnection()) {
            conn.resetSessionToCdb();
            conn.execute("INSERT INTO " + getFlushTableName() + " values (" + scnValue + ")");
        }
    }

    private static String getFlushTableName() {
        return TestHelper.getConnectorUserName() + "." + LOGMNR_FLUSH_TABLE;
    }
}

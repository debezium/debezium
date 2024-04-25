/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
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
import io.debezium.connector.oracle.junit.SkipOnReadOnly;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnDatabaseOptionRule;
import io.debezium.connector.oracle.junit.SkipTestDependingOnReadOnly;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Flush strategy only applies to LogMiner implementation")
@SkipOnReadOnly(reason = "Test expects flush table, not applicable during read only.")
public class FlushStrategyIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();
    @Rule
    public final TestRule skipOptionRule = new SkipTestDependingOnDatabaseOptionRule();
    @Rule
    public final TestRule skipReadOnly = new SkipTestDependingOnReadOnly();

    private static OracleConnection connection;
    private static String flushTableName;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        flushTableName = TestHelper.defaultConfig().build().getString(OracleConnectorConfig.LOG_MINING_FLUSH_TABLE_NAME);
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
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
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
            dropFlushTable(config);

            // Start connector
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Use a single insert as a marker entry to know when its safe to test flush strategy table
            connection.execute("INSERT INTO dbz4118 (id,data) values (1,'Test')");
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4118")).hasSize(1);

            // Verify only one row after a record is captured in streaming loop
            assertFlushTableHasExactlyOneRow(config);

            // Restart the connector to simulate an existing connector
            stopConnector();

            // Insert a second row into flush table
            insertFlushTable(config, "12345");

            LogInterceptor logInterceptor = new LogInterceptor(CommitLogWriterFlushStrategy.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Verify that the connector logged multiple rows detected and fixed
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> logInterceptor.containsWarnMessage(
                    "DBZ-4118: The flush table, " + flushTableName + ", has multiple rows"));

            // Verify that no additional rows get inserted on restart
            // Log entry will occur before the SQL has fired in the strategy, so delay checking to allow
            // the connector to have deleted and fixed the records before proceeding
            TestHelper.sleep(5, TimeUnit.SECONDS);
            assertFlushTableHasExactlyOneRow(config);

            // Use a single insert as a marker entry to know when its safe to test flush strategy table
            connection.execute("INSERT INTO dbz4118 (id,data) values (2,'Test')");
            records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ4118")).hasSize(1);

            // Verify only one row after a record is captured in streaming loop
            assertFlushTableHasExactlyOneRow(config);
        }
        finally {
            TestHelper.dropTable(connection, "dbz4118");
        }
    }

    private void assertFlushTableHasExactlyOneRow(Configuration config) throws SQLException {
        try (OracleConnection conn = TestHelper.defaultConnection(true)) {
            final String databasePdbName = config.getString(OracleConnectorConfig.PDB_NAME);
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                conn.setSessionToPdb(databasePdbName);
            }
            assertThat(conn.getRowCount(getFlushTableId())).isEqualTo(1L);
        }
    }

    private void dropFlushTable(Configuration config) throws SQLException {
        try (OracleConnection admin = TestHelper.adminConnection(true)) {
            final String databasePdbName = config.getString(OracleConnectorConfig.PDB_NAME);
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                admin.setSessionToPdb(databasePdbName);
            }
            TestHelper.dropTable(admin, getFlushTableName());
        }
    }

    private void insertFlushTable(Configuration config, String scnValue) throws SQLException {
        try (OracleConnection conn = TestHelper.defaultConnection(true)) {
            final String databasePdbName = config.getString(OracleConnectorConfig.PDB_NAME);
            if (!Strings.isNullOrEmpty(databasePdbName)) {
                conn.setSessionToPdb(databasePdbName);
            }
            conn.execute("INSERT INTO " + getFlushTableName() + " values (" + scnValue + ")");
        }
    }

    private static String getFlushTableName() {
        return TestHelper.getConnectorUserName() + "." + flushTableName;
    }

    private static TableId getFlushTableId() {
        return TableId.parse(getFlushTableName());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private OracleConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "a");
        TestHelper.dropTable(connection, "b");
        connection.execute("CREATE TABLE a (pk numeric(9,0) primary key, aa numeric(9,0))");
        connection.execute("CREATE TABLE b (pk numeric(9,0) primary key, aa numeric(9,0))");
        connection.execute("GRANT INSERT on a to " + TestHelper.getConnectorUserName());
        connection.execute("GRANT INSERT on b to " + TestHelper.getConnectorUserName());
        TestHelper.streamTable(connection, "a");
        TestHelper.streamTable(connection, "b");

        TestHelper.dropTable(connection, "debezium_signal");
        connection.execute("CREATE TABLE debezium_signal (id varchar2(64), type varchar2(32), data varchar2(2048))");
        connection.execute("GRANT INSERT on debezium_signal to " + TestHelper.getConnectorUserName());
        TestHelper.streamTable(connection, "debezium_signal");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "a");
            TestHelper.dropTable(connection, "b");
            TestHelper.dropTable(connection, "debezium_signal");
            connection.close();
        }
    }

    @Override
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        try {
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Class<OracleConnector> connectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.DEBEZIUM.A";
    }

    @Override
    protected List<String> topicNames() {
        return List.of("server1.DEBEZIUM.A", "server1.DEBEZIUM.B");
    }

    @Override
    protected String tableName() {
        return "DEBEZIUM.A";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("DEBEZIUM.A", "DEBEZIUM.B");
    }

    @Override
    protected String tableDataCollectionId() {
        return TestHelper.getDatabaseName() + ".DEBEZIUM.A";
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return List.of(TestHelper.getDatabaseName() + ".DEBEZIUM.A", TestHelper.getDatabaseName() + ".DEBEZIUM.B");
    }

    @Override
    protected String signalTableName() {
        return "DEBEZIUM.DEBEZIUM_SIGNAL";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL)
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.SCHEMA_INCLUDE_LIST, "DEBEZIUM")
                .with(OracleConnectorConfig.SNAPSHOT_MODE_TABLES, TestHelper.getDatabaseName() + ".DEBEZIUM.A")
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        return config();
    }

    @Override
    protected String valueFieldName() {
        return "AA";
    }

    @Override
    protected String pkFieldName() {
        return "PK";
    }

    @Override
    protected String alterTableAddColumnStatement(String tableName) {
        return "ALTER TABLE " + tableName + " ADD col3 INTEGER DEFAULT 0";
    }

    @Override
    protected String connector() {
        return "oracle";
    }

    @Override
    protected String server() {
        return TestHelper.SERVER_NAME;
    }

}

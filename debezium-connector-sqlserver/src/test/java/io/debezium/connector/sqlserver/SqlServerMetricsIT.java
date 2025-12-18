/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static io.debezium.connector.sqlserver.util.TestHelper.SCHEMA_HISTORY_PATH;
import static io.debezium.connector.sqlserver.util.TestHelper.TEST_DATABASE_1;
import static io.debezium.connector.sqlserver.util.TestHelper.TEST_SERVER_NAME;

import java.sql.SQLException;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.pipeline.AbstractMetricsTest;
import io.debezium.util.Testing;

public class SqlServerMetricsIT extends AbstractMetricsTest<SqlServerConnector> {

    @Override
    protected Class<SqlServerConnector> getConnectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TEST_SERVER_NAME;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL);
    }

    @Override
    protected Configuration.Builder noSnapshot(Configuration.Builder config) {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.NO_DATA);
    }

    @Override
    protected void executeInsertStatements() throws Exception {
        connection.execute("INSERT INTO tablea VALUES('a')", "INSERT INTO tablea VALUES('b')");
    }

    @Override
    protected String tableName() {
        return "testDB1.dbo.tablea";
    }

    @Override
    protected long expectedEvents() {
        return 2L;
    }

    @Override
    protected boolean snapshotCompleted() {
        return true;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TEST_DATABASE_1;
    }

    private SqlServerConnection connection;

    @BeforeEach
    void before() throws Exception {
        // Testing.Print.enable();
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int IDENTITY(1,1) primary key, cola varchar(30))");
        TestHelper.enableTableCdc(connection, "tablea");
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);

        // Be sure the agent is running
        TestHelper.waitForMaxLsnAvailable(connection, TEST_DATABASE_1);
    }

    @AfterEach
    void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return MetricsHelper.getSnapshotMetricsObjectName(connector(), server(), task(), TEST_DATABASE_1);
    }

    @Override
    protected ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return MetricsHelper.getStreamingMetricsObjectName(connector(), server(), getStreamingNamespace(), task());
    }

    @Override
    protected ObjectName getMultiplePartitionStreamingMetricsObjectName() throws MalformedObjectNameException {
        return MetricsHelper.getStreamingMetricsObjectName(connector(), server(), getStreamingNamespace(), task(), TEST_DATABASE_1);
    }

    @Override
    protected ObjectName getMultiplePartitionStreamingMetricsObjectNameCustomTags(Map<String, String> customTags) throws MalformedObjectNameException {
        return MetricsHelper.getStreamingMetricsObjectName(connector(), server(), task(), TEST_DATABASE_1, customTags);
    }

    @Override
    protected void assertStreamingStatistics(long expectedEvents) throws Exception {
        // no-op, AbstractSqlServerPartitionMetrics doesn't support all the metrics DefaultSnapshotChangeEventSourceMetrics does, including lagBehind Source,
        // so skip this test in SQL server connector now
    }

    @Test
    @Override
    public void testSnapshotAndStreamingMetrics() throws Exception {
        // Setup
        executeInsertStatements();

        // start connector
        start();
        assertConnectorIsRunning();

        assertSnapshotMetrics();

        consumeRecords(2);

        TestHelper.disableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.waitForEnabledCdc(connection, "tablea");

        assertStreamingMetrics(false, expectedEvents());
    }

    @Test
    @Override
    public void testSnapshotAndStreamingWithCustomMetrics() throws Exception {
        // Setup
        executeInsertStatements();

        // start connector

        Map<String, String> customMetricTags = Map.of("env", "test", "bu", "bigdata");
        start(x -> x.with(CommonConnectorConfig.CUSTOM_METRIC_TAGS, "env=test,bu=bigdata"));

        assertSnapshotWithCustomMetrics(customMetricTags);

        consumeRecords(2);

        TestHelper.disableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.waitForEnabledCdc(connection, "tablea");

        assertStreamingWithCustomMetrics(customMetricTags, expectedEvents());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import org.fest.assertions.Assertions;
import org.fest.assertions.MapAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<SqlServerConnector> {

    private SqlServerConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE a (pk int primary key, aa int)",
                "CREATE TABLE debezium_signal (id varchar(64), type varchar(32), data varchar(2048))");
        TestHelper.enableTableCdc(connection, "debezium_signal");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected void populateTable() throws SQLException {
        super.populateTable();
        TestHelper.enableTableCdc(connection, "a");
    }

    private void populateExcludedColumnsTable() throws SQLException {
        try (final JdbcConnection connection = databaseConnection()) {

            connection.setAutoCommit(false);

            connection.execute("CREATE TABLE excluded_column_table_a (id int, aa int, amount integer primary key(id))");

            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO excluded_column_table_a (id, aa, amount) VALUES (%s, %s, %s)", i + 1, i, i));
            }
            connection.commit();
        }

        TestHelper.enableTableCdc(connection, "excluded_column_table_a", "dbo_excluded_column_table_a",
                Arrays.asList("id", "aa"));
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "server1.dbo.a";
    }

    @Override
    protected String tableName() {
        return "testDB.dbo.a";
    }

    @Override
    protected String signalTableName() {
        return "dbo.debezium_signal";
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.SIGNAL_DATA_COLLECTION, "testDB.dbo.debezium_signal");
    }

    @Test
    @FixFor("DBZ-4243")
    public void whenCaptureInstanceExcludesColumnsExpectSnapshotAndStreamingToExcludeColumns() throws Exception {
        populateExcludedColumnsTable();

        startConnector();

        sendAdHocSnapshotSignal("testDB.dbo.excluded_column_table_a");

        Thread.sleep(5000);

        final int expectedRecordCount = ROW_COUNT;
        final Map<Integer, Integer> dbChanges = consumeMixedWithIncrementalSnapshot(
                expectedRecordCount,
                x -> true,
                k -> k.getInt32("id"),
                "server1.dbo.excluded_column_table_a",
                null);
        for (int i = 0; i < expectedRecordCount; i++) {
            Assertions.assertThat(dbChanges).includes(MapAssert.entry(i + 1, i));
        }
    }
}

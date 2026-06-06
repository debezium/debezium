/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.util.Testing;

/**
 * SQL Server-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class SqlServerChunkedSnapshotIT extends AbstractChunkedSnapshotTest<SqlServerConnector> {

    private SqlServerConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.afterEach();
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
        TestHelper.enableTableCdc(connection, tableName);
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
        TestHelper.enableTableCdc(connection, tableName);
    }

    @Override
    protected Class<SqlServerConnector> getConnectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig();
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        TestHelper.waitForSnapshotToBeCompleted();
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        TestHelper.waitForStreamingStarted();
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "dbo.dbz1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("dbo.dbz1220a", "dbo.dbz1220b", "dbo.dbz1220c", "dbo.dbz1220d"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar(50))".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), org_name varchar(50), data varchar(50), primary key(id, org_name))".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), data varchar(50))".formatted(tableName));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "id";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("id", "org_name");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return "server1.%s.dbo.%s".formatted(TestHelper.TEST_DATABASE_1, tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "%s.dbo.%s".formatted(TestHelper.TEST_DATABASE_1, tableName);
    }
}

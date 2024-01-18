/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;
import io.debezium.util.Testing;

/**
 * SQL Server integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public class SqlServerReselectColumnsProcessorIT extends AbstractReselectProcessorTest<SqlServerConnector> {

    private SqlServerConnection connection;

    @Before
    public void beforeEach() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.setAutoCommit(false);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        super.afterEach();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected Class<SqlServerConnector> getConnectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo\\.dbz4321")
                .with(SqlServerConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return "server1.testDB1.dbo.dbz4321";
    }

    @Override
    protected String tableName() {
        return "dbo.dbz4321";
    }

    @Override
    protected String reselectColumnsList() {
        return tableName() + ":data";
    }

    @Override
    protected void createTable() throws Exception {
        connection.execute("CREATE TABLE dbz4321 (id int identity(1,1) primary key, data varchar(50), data2 int)");
        TestHelper.enableTableCdc(connection, "dbz4321");
    }

    @Override
    protected void dropTable() throws Exception {
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbo.dbz4321 (data,data2) values ('one',1)";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbo.dbz4321 (data,data2) values (null,1)";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        TestHelper.waitForStreamingStarted();
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNullSnapshot() throws InterruptedException {
        // The second one is because the change gets captured by the table CDC
        // since the table's CDC was enabled before the snapshot.
        return consumeRecordsByTopic(3);
    }

}

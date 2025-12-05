/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

/**
 * Binlog-based connector integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public abstract class BinlogReselectColumnsProcessorIT<C extends SourceConnector>
        extends AbstractReselectProcessorTest<C>
        implements BinlogConnectorTest<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files
            .createTestingPath("file-schema-history-reselect-processor.txt").toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("processor", "empty")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private BinlogTestConnection connection;

    @Before
    public void beforeEach() throws Exception {
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
        connection = getTestDatabaseConnection(DATABASE.getDatabaseName());
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        super.afterEach();
        if (connection != null) {
            connection.close();
        }
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz4321"))
                .with(BinlogConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("dbz4321");
    }

    @Override
    protected String tableName() {
        return DATABASE.qualifiedTableName("dbz4321");
    }

    @Override
    protected String reselectColumnsList() {
        return DATABASE.qualifiedTableName("dbz4321") + ":data";
    }

    @Override
    protected void createTable() throws Exception {
        connection.execute("CREATE TABLE dbz4321 (id int primary key, data varchar(50), data2 int);");
    }

    @Override
    protected void dropTable() throws Exception {
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,'one',1);";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,null,1);";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullSnapshot() throws InterruptedException {
        return consumeRecordsByTopic(7);
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullStreaming() throws InterruptedException {
        return consumeRecordsByTopic(10);
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNullSnapshot() throws InterruptedException {
        return consumeRecordsByTopic(7);
    }

    @Override
    protected SourceRecords consumeRecordsByTopicReselectWhenNullStreaming() throws InterruptedException {
        return consumeRecordsByTopic(8);
    }
}

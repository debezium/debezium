/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;

/**
 * Abstract binlog chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public abstract class BinlogChunkedSnapshotIT<T extends SourceConnector>
        extends AbstractChunkedSnapshotTest<T>
        implements BinlogConnectorTest<T> {

    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history.txt").toAbsolutePath();

    protected final String SERVER_NAME = "ps_test";
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "chunked_snapshot_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    protected BinlogTestConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);

        connection = getTestDatabaseConnection(DATABASE.getDatabaseName());
        if (connection.connection().getAutoCommit()) {
            // todo:
            // for some reason enabling auto-commit here creates issues for other test classes
            // that come after this test class; despite the fact of whether the buffer size is
            // set to 0 to disable it or if we use auto-commit within try-with-resources areas
            // that perform bulk database operations. For now, disabling this, as the only
            // impact is that loading data in the tests takes significantly longer.
            // Makes sure that when we do large bulk inserts, the performance is optimal
            // and the inserts are all part of a singular transaction.
            // connection.setAutoCommit(false);
        }

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
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return DATABASE.qualifiedTableName("dbz1220");
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of(
                DATABASE.qualifiedTableName("dbz1220a"),
                DATABASE.qualifiedTableName("dbz1220b"),
                DATABASE.qualifiedTableName("dbz1220c"),
                DATABASE.qualifiedTableName("dbz1220d")));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int primary key, data varchar(50))".formatted(DATABASE.qualifiedTableName(tableName)));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection
                .execute("CREATE TABLE %s (id int, org_name varchar(50), data varchar(50), primary key(id, org_name))".formatted(DATABASE.qualifiedTableName(tableName)));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id int, data varchar(50))".formatted(DATABASE.qualifiedTableName(tableName)));
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
        return DATABASE.topicForTable(tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return DATABASE.qualifiedTableName(tableName);
    }
}

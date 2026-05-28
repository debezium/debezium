/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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
            connection.setAutoCommit(false);
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

    /**
     * Regression test for DBZ-1988: rowCountForTableChunked uses getQualifiedTableName (unquoted)
     * instead of quotedTableIdString, causing a SQL syntax error when the database name contains
     * hyphens (e.g. "my-db" becomes the expression `my - db` in MySQL).
     */
    @FixFor("debezium/dbz#1988")
    @Test
    public void shouldSnapshotTableInDatabaseWithHyphenatedName() throws Exception {
        final String hyphenDb = "debezium-hyphen-test";
        final int rowCount = 10;

        try {
            connection.execute(
                    "CREATE DATABASE IF NOT EXISTS `" + hyphenDb + "`",
                    "CREATE TABLE `" + hyphenDb + "`.`items` (id INT PRIMARY KEY, name VARCHAR(50))",
                    "INSERT INTO `" + hyphenDb + "`.`items` VALUES " +
                            "(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')");

            final Configuration config = DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, hyphenDb)
                    .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, hyphenDb + ".items")
                    .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                    .build();

            start(getConnectorClass(), config);
            assertConnectorIsRunning();
            waitForSnapshotToBeCompleted();

            final var records = consumeRecordsByTopic(rowCount);
            assertThat(records.allRecordsInOrder()).hasSize(rowCount);
        }
        finally {
            connection.execute("DROP DATABASE IF EXISTS `" + hyphenDb + "`");
            stopConnector();
        }
    }
}

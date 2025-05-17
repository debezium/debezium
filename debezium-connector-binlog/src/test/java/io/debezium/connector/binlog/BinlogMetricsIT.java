/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.pipeline.AbstractMetricsTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.file.history.FileSchemaHistory;

/**
 * @author Chris Cranford
 */
public abstract class BinlogMetricsIT<C extends SourceConnector> extends AbstractMetricsTest<C> implements BinlogConnectorTest<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-metrics.txt").toAbsolutePath();
    private static final String SERVER_NAME = "myserver";
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "connector_metrics_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private static final String INSERT1 = "INSERT INTO simple (val) VALUES (25);";
    private static final String INSERT2 = "INSERT INTO simple (val) VALUES (50);";

    @Override
    protected String connector() {
        return getConnectorName();
    }

    @Override
    protected String server() {
        return SERVER_NAME;
    }

    @Override
    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("simple"))
                .with(BinlogConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE);
    }

    protected Configuration.Builder noSnapshot(Configuration.Builder config) {
        return config.with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected void executeInsertStatements() throws SQLException {
        try (Connection connection = getTestDatabaseConnection(DATABASE.getDatabaseName()).connection()) {
            connection.createStatement().execute(INSERT1);
            connection.createStatement().execute(INSERT2);
        }
    }

    @Override
    protected String tableName() {
        return DATABASE.qualifiedTableName("simple");
    }

    @Override
    protected long expectedEvents() {
        return 2L;
    }

    @Override
    protected boolean snapshotCompleted() {
        return true;
    }

    @Before
    public void before() throws Exception {
        // Testing.Print.enable();
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }

        dropAllDatabases();
    }

    protected ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return getSnapshotMetricsObjectName(getConnectorName(), SERVER_NAME);
    }

    public ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return getStreamingMetricsObjectName(getConnectorName(), SERVER_NAME, getStreamingNamespace());
    }

}

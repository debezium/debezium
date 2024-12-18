/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * @author Chris Cranford
 */
public abstract class BinlogNotificationsIT<C extends SourceConnector> extends AbstractNotificationsIT<C> implements BinlogConnectorTest<C> {

    protected static final String SERVER_NAME = "is_test";
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase(SERVER_NAME, "incremental_snapshot-test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-is.txt")
            .toAbsolutePath();

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    protected List<String> collections() {
        return List.of("a", "b", "c", "a4",
                "a42", "a_dt", "a_date", "debezium_signal").stream().map(tbl -> String.format("%s.%s", DATABASE.getDatabaseName(), tbl)).collect(Collectors.toList());
    }

    @Override
    protected Class<C> connectorClass() {
        return getConnectorClass();
    }

    @Override
    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    @Override
    protected String connector() {
        return getConnectorName();
    }

    @Override
    protected String server() {
        return DATABASE.getServerName();
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}

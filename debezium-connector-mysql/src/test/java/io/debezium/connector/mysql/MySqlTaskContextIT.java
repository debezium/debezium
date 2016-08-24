/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class MySqlTaskContextIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-context.txt").toAbsolutePath();

    private String hostname;
    private int port;
    private String username;
    private String password;
    private int serverId;
    private String serverName;
    private String databaseName;

    private Configuration config;
    private MySqlTaskContext context;

    @Before
    public void beforeEach() {
        hostname = System.getProperty("database.hostname");
        port = Integer.parseInt(System.getProperty("database.port"));
        username = "snapper";
        password = "snapperpass";
        serverId = 18965;
        serverName = "logical_server_name";
        databaseName = "connector_test_ro";
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        if (context != null) {
            try {
                context.shutdown();
            } finally {
                context = null;
                Testing.Files.delete(DB_HISTORY_PATH);
            }
        }
    }

    protected Configuration.Builder simpleConfig() {
        return Configuration.create()
                            .with(MySqlConnectorConfig.HOSTNAME, hostname)
                            .with(MySqlConnectorConfig.PORT, port)
                            .with(MySqlConnectorConfig.USER, username)
                            .with(MySqlConnectorConfig.PASSWORD, password)
                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                            .with(MySqlConnectorConfig.SERVER_ID, serverId)
                            .with(MySqlConnectorConfig.SERVER_NAME, serverName)
                            .with(MySqlConnectorConfig.DATABASE_WHITELIST, databaseName)
                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    @Test
    public void shouldCreateTaskFromConfiguration() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config);
        context.start();
        assertThat(context.config()).isSameAs(config);

        assertThat(context.clock()).isNotNull();
        assertThat(context.dbSchema()).isNotNull();
        assertThat(context.jdbc()).isNotNull();
        assertThat(context.logger()).isNotNull();
        assertThat(context.makeRecord()).isNotNull();
        assertThat(context.source()).isNotNull();
        assertThat(context.topicSelector()).isNotNull();

        assertThat(context.hostname()).isEqualTo(hostname);
        assertThat(context.port()).isEqualTo(port);
        assertThat(context.username()).isEqualTo(username);
        assertThat(context.password()).isEqualTo(password);
        assertThat(context.serverId()).isEqualTo(serverId);
        assertThat(context.serverName()).isEqualTo(serverName);

        assertThat("" + context.includeSchemaChangeRecords()).isEqualTo(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES.defaultValueAsString());
        assertThat("" + context.maxBatchSize()).isEqualTo(MySqlConnectorConfig.MAX_BATCH_SIZE.defaultValueAsString());
        assertThat("" + context.maxQueueSize()).isEqualTo(MySqlConnectorConfig.MAX_QUEUE_SIZE.defaultValueAsString());
        assertThat("" + context.pollIntervalInMillseconds()).isEqualTo(MySqlConnectorConfig.POLL_INTERVAL_MS.defaultValueAsString());
        assertThat("" + context.snapshotMode().getValue()).isEqualTo(MySqlConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());

        // Snapshot default is 'initial' ...
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);

        assertNotConnectedToJdbc();
    }

    @Test
    public void shouldCreateTaskFromConfigurationWithNeverSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.NEVER.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(true);
    }

    @Test
    public void shouldCreateTaskFromConfigurationWithWhenNeededSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED.getValue())
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.WHEN_NEEDED.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(true);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);
    }

    @Test
    public void shouldCloseJdbcConnectionOnShutdown() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config);
        context.start();

        assertNotConnectedToJdbc();
        context.jdbc().connection(); // this should establish a connection
        assertConnectedToJdbc();

        context.shutdown();
        assertNotConnectedToJdbc();
    }

    protected void assertCanConnectToJdbc() throws SQLException {
        AtomicInteger count = new AtomicInteger();
        context.jdbc().query("SHOW DATABASES", rs -> {
            while (rs.next())
                count.incrementAndGet();
        });
        assertThat(count.get()).isGreaterThan(0);
    }

    protected void assertConnectedToJdbc() throws SQLException {
        assertThat(context.jdbc().isConnected()).isTrue();
    }

    protected void assertNotConnectedToJdbc() throws SQLException {
        assertThat(context.jdbc().isConnected()).isFalse();
    }
}

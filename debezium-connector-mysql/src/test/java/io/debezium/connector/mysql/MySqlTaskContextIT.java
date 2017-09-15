/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.debezium.config.Configuration;
import io.debezium.connector.cube.DatabaseCube;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.cube.DefaultDatabase;
import io.debezium.relational.history.FileDatabaseHistory;

/**
 * @author Randall Hauch
 *
 */
@RunWith(Arquillian.class)
public class MySqlTaskContextIT extends MySqlTaskContextTest {

    @DefaultDatabase
    private DatabaseCube cube;

    protected Configuration.Builder simpleConfig() {
        return cube.configuration()
                            .with(MySqlConnectorConfig.USER, username)
                            .with(MySqlConnectorConfig.PASSWORD, password)
                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
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

        assertThat(context.hostname()).isEqualTo(cube.getHost());
        assertThat(context.port()).isEqualTo(cube.getPort());
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

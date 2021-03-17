/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.debezium.connector.mysql.MySqlConnectorConfig;

/**
 * @author Randall Hauch
 *
 */
public class MySqlTaskContextIT extends MySqlTaskContextTest {

    @Test
    public void shouldCreateTaskFromConfiguration() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();
        assertThat(context.config()).isSameAs(config);

        assertThat(context.getClock()).isNotNull();
        assertThat(context.dbSchema()).isNotNull();
        assertThat(context.getConnectionContext().jdbc()).isNotNull();
        assertThat(context.getConnectionContext().logger()).isNotNull();
        assertThat(context.makeRecord()).isNotNull();
        assertThat(context.source()).isNotNull();
        assertThat(context.topicSelector()).isNotNull();

        assertThat(context.getConnectionContext().hostname()).isEqualTo(hostname);
        assertThat(context.getConnectionContext().port()).isEqualTo(port);
        assertThat(context.getConnectionContext().username()).isEqualTo(username);
        assertThat(context.getConnectionContext().password()).isEqualTo(password);
        assertThat(context.serverId()).isEqualTo(serverId);
        assertThat(context.getConnectorConfig().getLogicalName()).isEqualTo(serverName);

        assertThat("" + context.includeSchemaChangeRecords()).isEqualTo(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES.defaultValueAsString());
        assertThat("" + context.includeSqlQuery()).isEqualTo(MySqlConnectorConfig.INCLUDE_SQL_QUERY.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getMaxBatchSize()).isEqualTo(MySqlConnectorConfig.MAX_BATCH_SIZE.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getMaxQueueSize()).isEqualTo(MySqlConnectorConfig.MAX_QUEUE_SIZE.defaultValueAsString());
        assertThat("" + context.getConnectorConfig().getPollInterval().toMillis()).isEqualTo(MySqlConnectorConfig.POLL_INTERVAL_MS.defaultValueAsString());
        assertThat("" + context.snapshotMode().getValue()).isEqualTo(MySqlConnectorConfig.SNAPSHOT_MODE.defaultValueAsString());

        // Snapshot default is 'initial' ...
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);

        // JDBC connection is automatically created by MySqlTaskContext when it reads database variables
        assertConnectedToJdbc();
    }

    @Test
    public void shouldCloseJdbcConnectionOnShutdown() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        context.start();

        // JDBC connection is automatically created by MySqlTaskContext when it reads database variables
        assertConnectedToJdbc();

        context.shutdown();
        assertNotConnectedToJdbc();
    }

    protected void assertCanConnectToJdbc() throws SQLException {
        AtomicInteger count = new AtomicInteger();
        context.getConnectionContext().jdbc().query("SHOW DATABASES", rs -> {
            while (rs.next()) {
                count.incrementAndGet();
            }
        });
        assertThat(count.get()).isGreaterThan(0);
    }

    protected void assertConnectedToJdbc() throws SQLException {
        assertThat(context.getConnectionContext().jdbc().isConnected()).isTrue();
    }

    protected void assertNotConnectedToJdbc() throws SQLException {
        assertThat(context.getConnectionContext().jdbc().isConnected()).isFalse();
    }
}

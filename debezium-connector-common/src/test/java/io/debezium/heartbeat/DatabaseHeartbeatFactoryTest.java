/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.heartbeat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Unit tests for {@link DatabaseHeartbeatFactory}.
 *
 * @author Chris Cranford
 */
class DatabaseHeartbeatFactoryTest {

    private final DatabaseHeartbeatFactory factory = new DatabaseHeartbeatFactory();

    @Test
    void shouldReturnEmptyWhenConnectionProviderIsNull() {
        final RelationalDatabaseConnectorConfig config = mock(RelationalDatabaseConnectorConfig.class);
        when(config.getHeartbeatActionQuery()).thenReturn("SELECT 1");

        final Optional<Heartbeat> heartbeat = factory.getHeartbeat(config, null, null, null);

        assertThat(heartbeat).isEmpty();
    }

    @Test
    void shouldReturnHeartbeatWhenConnectionProviderIsNotNull() {
        final RelationalDatabaseConnectorConfig config = mock(RelationalDatabaseConnectorConfig.class);
        when(config.getHeartbeatActionQuery()).thenReturn("SELECT 1");

        final JdbcConnection connection = mock(JdbcConnection.class);
        final Optional<Heartbeat> heartbeat = factory.getHeartbeat(config, () -> connection, null, null);

        assertThat(heartbeat).isPresent();
    }

    @Test
    void shouldReturnEmptyWhenActionQueryIsNotSet() {
        final RelationalDatabaseConnectorConfig config = mock(RelationalDatabaseConnectorConfig.class);
        when(config.getHeartbeatActionQuery()).thenReturn("");

        final Optional<Heartbeat> heartbeat = factory.getHeartbeat(config, null, null, null);

        assertThat(heartbeat).isEmpty();
    }

    @Test
    void shouldReturnEmptyWhenConfigIsNotRelational() {
        final CommonConnectorConfig config = mock(CommonConnectorConfig.class);

        final Optional<Heartbeat> heartbeat = factory.getHeartbeat(config, null, null, null);

        assertThat(heartbeat).isEmpty();
    }
}

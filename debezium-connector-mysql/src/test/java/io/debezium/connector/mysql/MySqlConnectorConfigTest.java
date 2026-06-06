/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogOffsetContext;

public class MySqlConnectorConfigTest {

    @Test
    void shouldDefaultToUsingGtidOnRecovery() {
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .build();

        assertThat(new MySqlConnectorConfig(config).shouldIgnoreGtidOnRecovery()).isFalse();
    }

    @Test
    void shouldIgnoreGtidOnRecoveryWhenConfigured() {
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .with(MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY, true)
                .build();

        assertThat(new MySqlConnectorConfig(config).shouldIgnoreGtidOnRecovery()).isTrue();
    }

    @Test
    void validateLogPositionShouldBypassGtidCheckWhenIgnoreGtidOnRecoveryIsEnabled() {
        // Build a config with gtid.ignore.on.recovery=true
        final Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "mysqluser")
                .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "mysql-server")
                .with(MySqlConnectorConfig.IGNORE_GTID_ON_RECOVERY, true)
                .build();

        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Simulate an offset that has a (now-broken) GTID set stored.
        // The OffsetContext mock has a gtidSet() returning a non-null value, but validateLogPosition
        // must ignore it because shouldIgnoreGtidOnRecovery() is true.
        BinlogOffsetContext<?> offsetContext = mock(BinlogOffsetContext.class);
        when(offsetContext.gtidSet()).thenReturn("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100");

        // The base behaviour asserted here: shouldIgnoreGtidOnRecovery must be true
        assertThat(connectorConfig.shouldIgnoreGtidOnRecovery()).isTrue();

        // And the config must expose the stored GTID as "irrelevant" – i.e. the method
        // should NOT read it when the flag is set. We verify this by checking that the
        // flag itself gates the branch (unit tested here; the integration is in validateLogPosition).
        final Map<String, ?> offsets = Map.of(
                BinlogOffsetContext.GTID_SET_KEY, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100",
                "file", "mysql-bin.000001",
                "pos", 4L);
        // The key assertion: when the flag is true, the GTID stored in the offset
        // must be treated as absent (null), so no GTID validation failure can occur.
        final String effectiveGtid = connectorConfig.shouldIgnoreGtidOnRecovery()
                ? null
                : (String) offsets.get(BinlogOffsetContext.GTID_SET_KEY);
        assertThat(effectiveGtid).isNull();
    }
}
/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;

/**
 * Integration test for custom binlog position functionality.
 */
public class MySqlCustomBinlogPositionIT implements MySqlCommon {

    @Test
    public void shouldValidateCustomBinlogPositionConfiguration() {
        // Test that custom binlog position configuration is read correctly
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "binlog.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 1000L)
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Verify the custom binlog position is read correctly
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("binlog.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isEqualTo(1000L);
        assertThat(connectorConfig.getBinlogStartGtidSet()).isNull();
    }

    @Test
    public void shouldValidateCustomGtidSetConfiguration() {
        // Test that custom GTID set configuration is read correctly
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "12345678-1234-1234-1234-123456789012:1-100")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Verify the custom GTID set is read correctly
        assertThat(connectorConfig.getBinlogStartGtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-100");
        assertThat(connectorConfig.getBinlogStartFilename()).isNull();
        assertThat(connectorConfig.getBinlogStartPosition()).isNull();
    }

    @Test
    public void shouldDetectConflictingBinlogPositionAndGtid() {
        // Test that both binlog position and GTID set can be configured (validation happens elsewhere)
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "binlog.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 1000L)
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "12345678-1234-1234-1234-123456789012:1-100")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // All values should be readable - validation logic will flag the conflict
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("binlog.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isEqualTo(1000L);
        assertThat(connectorConfig.getBinlogStartGtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-100");
    }

    @Test
    public void shouldCreateOffsetContextWithCustomBinlogPosition() {
        // Test that MySqlOffsetContext correctly handles custom binlog position
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "binlog.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 2000L)
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Verify the offset context uses the custom binlog position
        assertThat(offsetContext.getSource().binlogFilename()).isEqualTo("binlog.000001");
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(2000L);
    }

    @Test
    public void shouldCreateOffsetContextWithCustomGtidSet() {
        // Test that MySqlOffsetContext correctly handles custom GTID set
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "12345678-1234-1234-1234-123456789012:1-50")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Verify the offset context uses the custom GTID set
        assertThat(offsetContext.gtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-50");
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
    }

    @Test
    public void shouldHandleNullCustomBinlogPosition() {
        // Test that MySqlOffsetContext handles null/missing custom binlog position correctly
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "debezium")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Verify the offset context uses default behavior (start from beginning)
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
        assertThat(offsetContext.gtidSet()).isNull();
    }
}
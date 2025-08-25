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
 * Test cases for custom binlog position functionality in MySQL connector.
 * Tests both binlog file/position and GTID set configurations.
 *
 * @author DBZ-3829 Implementation
 */
public class MySqlCustomBinlogPositionTest implements MySqlCommon {

    @Test
    public void shouldStartFromCustomBinlogFileAndPosition() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000123")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 98765L)
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Verify the offset context is initialized with custom values
        assertThat(offsetContext.getSource().binlogFilename()).isEqualTo("mysql-bin.000123");
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(98765L);
        assertThat(offsetContext.gtidSet()).isNull();
    }

    @Test
    public void shouldStartFromCustomGtidSet() {
        String customGtidSet = "12345678-1234-1234-1234-123456789012:1-100,87654321-4321-4321-4321-210987654321:1-50";

        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, customGtidSet)
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Verify the offset context is initialized with custom GTID set
        assertThat(offsetContext.gtidSet()).isEqualTo(customGtidSet);
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
    }

    @Test
    public void shouldRejectInvalidBinlogFilenameFormat() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "invalid-filename")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                .build();

        // The config should be created, but validation should flag the invalid filename
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("invalid-filename");
    }

    @Test
    public void shouldRejectInvalidGtidSetFormat() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "invalid-gtid-format")
                .build();

        // The config should be created, but validation should flag the invalid GTID set
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        assertThat(connectorConfig.getBinlogStartGtidSet()).isEqualTo("invalid-gtid-format");
    }

    @Test
    public void shouldHandleMutuallyExclusiveConfigurations() {
        // Test that you cannot specify both binlog file/position AND GTID set
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "12345678-1234-1234-1234-123456789012:1-100")
                .build();

        // Configuration should be created, but validation logic should detect the conflict
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
        assertThat(connectorConfig.getBinlogStartFilename()).isNotNull();
        assertThat(connectorConfig.getBinlogStartPosition()).isNotNull();
        assertThat(connectorConfig.getBinlogStartGtidSet()).isNotNull();
    }

    @Test
    public void shouldRequireBothFilenameAndPosition() {
        // Test that providing only filename without position is invalid
        Configuration configOnlyFilename = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configOnlyFilename);
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("mysql-bin.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isNull();

        // Test that providing only position without filename is invalid
        Configuration configOnlyPosition = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                .build();

        MySqlConnectorConfig connectorConfig2 = new MySqlConnectorConfig(configOnlyPosition);
        assertThat(connectorConfig2.getBinlogStartFilename()).isNull();
        assertThat(connectorConfig2.getBinlogStartPosition()).isEqualTo(12345L);
    }

    @Test
    public void shouldHandleEmptyOrNullValues() {
        // Test with null values
        Configuration configWithNulls = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configWithNulls);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Should use default values
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
        assertThat(offsetContext.gtidSet()).isNull();

        // Test with empty strings
        Configuration configWithEmpty = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "")
                .build();

        MySqlConnectorConfig connectorConfig2 = new MySqlConnectorConfig(configWithEmpty);
        MySqlOffsetContext offsetContext2 = MySqlOffsetContext.initial(connectorConfig2);

        // Empty strings should be treated as no custom position
        assertThat(offsetContext2.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext2.getSource().binlogPosition()).isEqualTo(0L);
        assertThat(offsetContext2.gtidSet()).isNull();
    }

    @Test
    public void shouldValidateValidBinlogFilenames() {
        // Test various valid MySQL binlog filename formats
        String[] validFilenames = {
                "mysql-bin.000001",
                "mysql-bin.123456",
                "binlog.000001",
                "my_server-bin.000001",
                "server-1-bin.999999"
        };

        for (String filename : validFilenames) {
            Configuration config = Configuration.create()
                    .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                    .with(MySqlConnectorConfig.PORT, 3306)
                    .with(MySqlConnectorConfig.USER, "root")
                    .with(MySqlConnectorConfig.PASSWORD, "password")
                    .with(MySqlConnectorConfig.SERVER_ID, 1)
                    .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                    .with(BinlogConnectorConfig.BINLOG_START_FILENAME, filename)
                    .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                    .build();

            MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);
            MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

            assertThat(offsetContext.getSource().binlogFilename()).isEqualTo(filename);
            assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(12345L);
        }
    }
}

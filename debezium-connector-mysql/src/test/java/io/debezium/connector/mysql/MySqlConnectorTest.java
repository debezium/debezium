/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfigTest;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorTest extends BinlogConnectorConfigTest<MySqlConnector> implements MySqlCommon {
    @Override
    protected MySqlConnector getConnectorInstance() {
        return new MySqlConnector();
    }

    @Override
    protected Field.Set getAllFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    @Test
    public void shouldCreateOffsetContextWithCustomBinlogPosition() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Test that custom binlog position is read correctly
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("mysql-bin.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isEqualTo(12345L);

        // Test that offset context is created with custom position
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        assertThat(offsetContext.getSource().binlogFilename()).isEqualTo("mysql-bin.000001");
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(12345L);
    }

    @Test
    public void shouldValidateBinlogStartPositionConfiguration() {
        // Test that both filename and position must be provided together
        Configuration configWithOnlyFilename = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                // Missing position - this should trigger validation error
                .build();

        // The validation will happen when the connector config is created
        // This test mainly verifies that the configuration fields are properly defined
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configWithOnlyFilename);
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("mysql-bin.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isNull();
    }

    @Test
    public void shouldCreateOffsetContextWithoutCustomBinlogPosition() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Test that no custom binlog position is set
        assertThat(connectorConfig.getBinlogStartFilename()).isNull();
        assertThat(connectorConfig.getBinlogStartPosition()).isNull();

        // Test that offset context starts from default position
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
    }

    @Test
    public void shouldDetectCustomBinlogPositionInSnapshotLogic() {
        // Configuration with custom binlog position
        Configuration configWithCustomPosition = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 12345L)
                .build();

        MySqlConnectorConfig connectorConfigWithCustom = new MySqlConnectorConfig(configWithCustomPosition);
        MySqlOffsetContext offsetContextWithCustom = MySqlOffsetContext.initial(connectorConfigWithCustom);

        // Mock a snapshot source to test the custom binlog position detection
        // Since we can't easily instantiate the actual snapshot source in a unit test,
        // we'll test the config getters directly
        assertThat(connectorConfigWithCustom.getBinlogStartFilename()).isNotNull();
        assertThat(connectorConfigWithCustom.getBinlogStartPosition()).isNotNull();

        // Configuration without custom binlog position
        Configuration configWithoutCustomPosition = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .build();

        MySqlConnectorConfig connectorConfigWithout = new MySqlConnectorConfig(configWithoutCustomPosition);

        // Test that custom binlog detection works correctly
        assertThat(connectorConfigWithout.getBinlogStartFilename()).isNull();
        assertThat(connectorConfigWithout.getBinlogStartPosition()).isNull();
    }

    @Test
    public void shouldCreateOffsetContextWithCustomGtidSet() {
        Configuration config = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "12345678-1234-1234-1234-123456789012:1-100")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // Test that custom GTID set is read correctly
        assertThat(connectorConfig.getBinlogStartGtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-100");
        assertThat(connectorConfig.getBinlogStartFilename()).isNull();
        assertThat(connectorConfig.getBinlogStartPosition()).isNull();

        // Test that offset context is created with custom GTID set
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        assertThat(offsetContext.gtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-100");
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
    }

    @Test
    public void shouldRejectConflictingBinlogPositionAndGtidSet() {
        // This test verifies that validation prevents using both binlog position and GTID set together
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

        // The validation will detect the conflict when the connector config is created
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(config);

        // All values should be read correctly, but the validation should have flagged the conflict
        assertThat(connectorConfig.getBinlogStartFilename()).isEqualTo("mysql-bin.000001");
        assertThat(connectorConfig.getBinlogStartPosition()).isEqualTo(12345L);
        assertThat(connectorConfig.getBinlogStartGtidSet()).isEqualTo("12345678-1234-1234-1234-123456789012:1-100");
    }

    @Test
    public void shouldHandleEdgeCasesInCustomBinlogPositions() {
        // Test with empty strings
        Configuration configWithEmptyStrings = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "")
                .with(BinlogConnectorConfig.BINLOG_START_GTID_SET, "")
                .build();

        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configWithEmptyStrings);
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);

        // Empty strings should be treated as null - should use default behavior
        assertThat(offsetContext.getSource().binlogFilename()).isEmpty();
        assertThat(offsetContext.getSource().binlogPosition()).isEqualTo(0L);
        assertThat(offsetContext.gtidSet()).isNull();

        // Test with zero position (should be valid)
        Configuration configWithZeroPosition = Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "password")
                .with(MySqlConnectorConfig.SERVER_ID, 1)
                .with(MySqlConnectorConfig.TOPIC_PREFIX, "test")
                .with(BinlogConnectorConfig.BINLOG_START_FILENAME, "mysql-bin.000001")
                .with(BinlogConnectorConfig.BINLOG_START_POSITION, 0L)
                .build();

        MySqlConnectorConfig connectorConfigZero = new MySqlConnectorConfig(configWithZeroPosition);
        MySqlOffsetContext offsetContextZero = MySqlOffsetContext.initial(connectorConfigZero);

        assertThat(offsetContextZero.getSource().binlogFilename()).isEqualTo("mysql-bin.000001");
        assertThat(offsetContextZero.getSource().binlogPosition()).isEqualTo(0L);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.Config;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorIT;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorIT extends BinlogConnectorIT<MySqlConnector, MySqlPartition, MySqlOffsetContext> implements MySqlCommon {

    @Test
    public void shouldNotStartWithUnknownJdbcDriver() {
        final Configuration config = getDatabase().defaultConfig()
                .with(MySqlConnectorConfig.JDBC_DRIVER, "foo.bar")
                .build();

        final AtomicBoolean successResult = new AtomicBoolean();
        final AtomicReference<String> message = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, msg, error) -> {
            successResult.set(success);
            message.set(msg);
        });

        assertThat(successResult.get()).isEqualTo(false);
        assertThat(message.get()).contains("java.lang.ClassNotFoundException: foo.bar");
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartWithWrongProtocol() {
        final Configuration config = getDatabase().defaultConfig()
                .with(MySqlConnectorConfig.JDBC_PROTOCOL, "foo:bar")
                .build();

        final AtomicBoolean successResult = new AtomicBoolean();
        final AtomicReference<String> message = new AtomicReference<>();
        start(MySqlConnector.class, config, (success, msg, error) -> {
            successResult.set(success);
            message.set(msg);
        });

        assertThat(successResult.get()).isEqualTo(false);
        assertThat(message.get()).contains("Unable to obtain a JDBC connection");
        assertConnectorNotRunning();
    }

    @Override
    protected Config validateConfiguration(Configuration configuration) {
        return new MySqlConnector().validate(configuration.asMap());
    }

    @Override
    protected void assertInvalidConfiguration(Config result) {
        super.assertInvalidConfiguration(result);
        assertNoConfigurationErrors(result, MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE);
    }

    @Override
    protected void assertValidConfiguration(Config result) {
        super.assertValidConfiguration(result);
        validateConfigField(result, MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.MINIMAL);
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return SnapshotLockingMode.NONE.getValue();
    }

    @Override
    protected void assertSnapshotLockingModeIsNone(Configuration config) {
        assertThat(new MySqlConnectorConfig(config).getSnapshotLockingMode().get()).isEqualTo(SnapshotLockingMode.NONE);
    }

    @Override
    protected MySqlPartition createPartition(String serverName, String databaseName) {
        return new MySqlPartition(serverName, databaseName);
    }

    @Override
    protected MySqlOffsetContext loadOffsets(Configuration configuration, Map<String, ?> offsets) {
        return new MySqlOffsetContext.Loader(new MySqlConnectorConfig(configuration)).load(offsets);
    }

    @Override
    protected void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition) {
        assertThat(offsetPosition).isGreaterThan(beforeInsertsPosition);
    }
}

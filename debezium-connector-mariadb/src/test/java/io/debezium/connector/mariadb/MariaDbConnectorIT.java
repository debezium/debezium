/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.common.config.Config;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorIT;
import io.debezium.connector.mariadb.MariaDbConnectorConfig.SnapshotLockingMode;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectorIT extends BinlogConnectorIT<MariaDbConnector, MariaDbPartition, MariaDbOffsetContext>
        implements MariaDbCommon {

    @Override
    protected Config validateConfiguration(Configuration configuration) {
        return new MariaDbConnector().validate(configuration.asMap());
    }

    @Override
    protected void assertInvalidConfiguration(Config result) {
        super.assertInvalidConfiguration(result);
        assertNoConfigurationErrors(result, MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE);
    }

    @Override
    protected void assertValidConfiguration(Config result) {
        super.assertValidConfiguration(result);
        validateConfigField(result, MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE, SnapshotLockingMode.MINIMAL);
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return SnapshotLockingMode.NONE.getValue();
    }

    @Override
    protected void assertSnapshotLockingModeIsNone(Configuration config) {
        assertThat(new MariaDbConnectorConfig(config).getSnapshotLockingMode().get()).isEqualTo(SnapshotLockingMode.NONE);
    }

    @Override
    protected MariaDbPartition createPartition(String serverName, String databaseName) {
        return new MariaDbPartition(serverName, databaseName);
    }

    @Override
    protected MariaDbOffsetContext loadOffsets(Configuration configuration, Map<String, ?> offsets) {
        return new MariaDbOffsetContext.Loader(new MariaDbConnectorConfig(configuration)).load(offsets);
    }

    @Override
    protected void assertBinlogPosition(long offsetPosition, long beforeInsertsPosition) {
        assertThat(offsetPosition).isGreaterThanOrEqualTo(beforeInsertsPosition);
    }
}

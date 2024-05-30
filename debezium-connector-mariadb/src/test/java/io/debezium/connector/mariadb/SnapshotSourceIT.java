/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogSnapshotSourceIT;

/**
 * @author Chris Cranford
 */
public class SnapshotSourceIT extends BinlogSnapshotSourceIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Field getSnapshotLockingModeField() {
        return MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeMinimal() {
        return MariaDbConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return MariaDbConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }
}

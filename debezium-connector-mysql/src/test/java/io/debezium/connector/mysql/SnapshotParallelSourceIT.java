/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogSnapshotParallelSourceIT;

public class SnapshotParallelSourceIT extends BinlogSnapshotParallelSourceIT<MySqlConnector> implements MySqlCommon {
    @Override
    protected Field getSnapshotLockingModeField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeMinimal() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return MySqlConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }
}

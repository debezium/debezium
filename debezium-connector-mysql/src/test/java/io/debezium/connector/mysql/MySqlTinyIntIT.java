/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogTinyIntIT;

/**
 * Verify correct range of TINYINT.
 *
 * @author Jiri Pechanec
 */
public class MySqlTinyIntIT extends BinlogTinyIntIT<MySqlConnector> implements MySqlCommon {
    @Override
    protected Field getSnapshotLockingField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }
}

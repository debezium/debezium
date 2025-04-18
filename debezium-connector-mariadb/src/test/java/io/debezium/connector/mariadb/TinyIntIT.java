/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogTinyIntIT;

/**
 * @author Chris Cranford
 */
public class TinyIntIT extends BinlogTinyIntIT<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected Field getSnapshotLockingField() {
        return MariaDbConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.util.Map;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class MinimalSnapshotLock extends DefaultSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }
}

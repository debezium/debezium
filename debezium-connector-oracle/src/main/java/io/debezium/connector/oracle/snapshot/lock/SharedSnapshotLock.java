/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class SharedSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return OracleConnectorConfig.SnapshotLockingMode.SHARED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {

        String tableId = tableIds.iterator().next(); // For Oracle ww expect just one table at time.
        return Optional.of("LOCK TABLE " + tableId + " IN ROW SHARE MODE");
    }
}

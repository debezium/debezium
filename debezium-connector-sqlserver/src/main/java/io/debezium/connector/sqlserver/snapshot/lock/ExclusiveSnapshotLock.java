/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class ExclusiveSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return SqlServerConnectorConfig.SnapshotLockingMode.EXCLUSIVE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {

        String tableId = tableIds.iterator().next(); // For SqlServer ww expect just one table at time.

        return Optional.of("SELECT TOP(0) * FROM " + tableId + " WITH (TABLOCKX)");
    }
}

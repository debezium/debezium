/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.binlog.snapshot.lock;

import static io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotLockingMode.MINIMAL_AT_LEAST_ONCE;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.snapshot.spi.SnapshotLock;

public class MinimalAtLeastOnceSnapshotLock implements SnapshotLock {
    @Override
    public String name() {
        return MINIMAL_AT_LEAST_ONCE.getValue();
    }

    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("FLUSH TABLES WITH READ LOCK");
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }
}

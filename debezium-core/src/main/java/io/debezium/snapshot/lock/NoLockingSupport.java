/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.snapshot.spi.SnapshotLock;

public class NoLockingSupport implements SnapshotLock {

    public static final String NO_LOCKING_SUPPORT = "no_locking_support";

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public String name() {
        return NO_LOCKING_SUPPORT;
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.empty();
    }
}

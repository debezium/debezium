/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/**
 * This interface is used to determine the table lock mode used during schema snapshot
 *
 */
public interface SnapshotLock {

    /**
     * Returns a SQL statement for locking the given tables during snapshotting, if required by the specific snapshotter
     * implementation.
     */
    Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds);

}

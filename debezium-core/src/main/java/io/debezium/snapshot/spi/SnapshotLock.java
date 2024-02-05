/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.spi;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import io.debezium.service.Service;
import io.debezium.spi.common.Configurable;

/**
 * {@link SnapshotLock} is used to determine the table lock mode used during schema snapshot
 *
 * @author Mario Fiore Vitale
 */
public interface SnapshotLock extends Configurable, Service {

    /**
     * @return the name of the snapshot lock.
     *
     *
     */
    String name();

    /**
     * Returns a SQL statement for locking the given tables during snapshotting, if required by the specific snapshotter
     * implementation.
     */
    Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds); // TODO Evaluate with DBZ-7308 if this method should receive only the single table

}

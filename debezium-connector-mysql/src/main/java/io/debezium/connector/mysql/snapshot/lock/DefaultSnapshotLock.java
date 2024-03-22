/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public abstract class DefaultSnapshotLock {
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {
        return Optional.of("FLUSH TABLES WITH READ LOCK");
    }
}

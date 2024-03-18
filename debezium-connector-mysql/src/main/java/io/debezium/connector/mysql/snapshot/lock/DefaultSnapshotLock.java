/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.time.Duration;
import java.util.Optional;

public abstract class DefaultSnapshotLock {
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("FLUSH TABLES WITH READ LOCK");
    }
}

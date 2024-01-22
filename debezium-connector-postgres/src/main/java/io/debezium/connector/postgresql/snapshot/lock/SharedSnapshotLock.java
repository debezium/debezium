/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class SharedSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotLockingMode.SHARED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {

        String lineSeparator = System.lineSeparator();
        StringBuilder statements = new StringBuilder();
        statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
        // we're locking in ACCESS SHARE MODE to avoid concurrent schema changes while we're taking the snapshot
        // this does not prevent writes to the table, but prevents changes to the table's schema....
        // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
        tableIds.forEach(tableId -> statements.append("LOCK TABLE ")
                .append(tableId)
                .append(" IN ACCESS SHARE MODE;")
                .append(lineSeparator));

        return Optional.of(statements.toString());
    }
}

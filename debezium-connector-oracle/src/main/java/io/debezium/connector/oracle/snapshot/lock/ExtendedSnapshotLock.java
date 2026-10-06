/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * A {@link SnapshotLock} that holds a {@code SHARE} table lock for the duration of the snapshot.
 *
 * Unlike the {@code ROW SHARE} lock used by {@link SharedSnapshotLock}, a {@code SHARE} lock blocks
 * all DDL on the table, including Oracle non-blocking DDL such as {@code ALTER TABLE ADD COLUMN},
 * at the cost of also blocking writes to the table until the snapshot completes.
 *
 * @author Chris Cranford
 */
@ConnectorSpecific(connector = OracleConnector.class)
public class ExtendedSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return OracleConnectorConfig.SnapshotLockingMode.EXTENDED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of(String.format("LOCK TABLE %s IN SHARE MODE", tableId));
    }
}

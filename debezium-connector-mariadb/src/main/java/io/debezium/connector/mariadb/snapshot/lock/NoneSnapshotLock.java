/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * @author Chris Cranford
 */
@ConnectorSpecific(connector = MariaDbConnector.class)
public class NoneSnapshotLock implements SnapshotLock {
    @Override
    public String name() {
        return MariaDbConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.empty();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.snapshot.lock;

import java.time.Duration;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.mariadb.MariaDbConnector;

/**
 * @author Chris Cranford
 */
@ConnectorSpecific(connector = MariaDbConnector.class)
public abstract class DefaultSnapshotLock {
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("FLUSH TABLES WITH READ LOCK");
    }
}

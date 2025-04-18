/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.time.Duration;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.mysql.MySqlConnector;

@ConnectorSpecific(connector = MySqlConnector.class)
public abstract class DefaultSnapshotLock {
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("FLUSH TABLES WITH READ LOCK");
    }
}

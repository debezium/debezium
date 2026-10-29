/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.binlog.snapshot.lock.UnlockStatementProvider;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * Instance-level backup lock ({@code LOCK INSTANCE FOR BACKUP}, MySQL 8.0+): blocks DDL while permitting
 * concurrent DML, and requires the {@code BACKUP_ADMIN} privilege instead of {@code RELOAD}.
 */
@ConnectorSpecific(connector = MySqlConnector.class)
public class MinimalInstanceSnapshotLock implements SnapshotLock, UnlockStatementProvider {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL_INSTANCE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
        return Optional.of("LOCK INSTANCE FOR BACKUP");
    }

    @Override
    public String globalUnlockingStatement() {
        return "UNLOCK INSTANCE";
    }
}

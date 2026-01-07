/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;

@ConnectorSpecific(connector = MySqlConnector.class)
public class MinimalPerconaNoTableLocksSnapshotLock extends MinimalPerconaSnapshotLock {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL_PERCONA_NO_TABLE_LOCKS.getValue();
    }

}

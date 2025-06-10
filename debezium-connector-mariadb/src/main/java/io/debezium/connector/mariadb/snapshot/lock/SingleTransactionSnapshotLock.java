/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mariadb.snapshot.lock;

import java.util.Map;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = MariaDbConnector.class)
public class SingleTransactionSnapshotLock extends DefaultSnapshotLock implements SnapshotLock {
    @Override
    public String name() {
        return MariaDbConnectorConfig.SnapshotLockingMode.SINGLE_TRANSACTION.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotLock}.
 *
 * @author Mario Fiore Vitale
 */
public class PostgresSnapshotLockProvider extends SnapshotLockProvider {

    @Override
    public String snapshotLockingMode(BeanRegistry beanRegistry) {

        PostgresConnectorConfig postgresConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, PostgresConnectorConfig.class);

        return postgresConnectorConfig.snapshotLockingMode().getValue();
    }

}

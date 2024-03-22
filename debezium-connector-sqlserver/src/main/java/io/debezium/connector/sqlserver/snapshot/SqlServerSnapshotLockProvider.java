/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.spi.SnapshotLock;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link SnapshotLock}.
 *
 * @author Mario Fiore Vitale
 */
public class SqlServerSnapshotLockProvider extends SnapshotLockProvider {

    @Override
    public String snapshotLockingMode(BeanRegistry beanRegistry) {

        SqlServerConnectorConfig sqlServerConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, SqlServerConnectorConfig.class);

        return sqlServerConnectorConfig.getSnapshotLockingMode().getValue();
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class SqlServerSnapshotterServiceProvider extends SnapshotterServiceProvider {

    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        SqlServerConnectorConfig mySqlConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, SqlServerConnectorConfig.class);

        return mySqlConnectorConfig.getSnapshotMode().getValue();
    }
}

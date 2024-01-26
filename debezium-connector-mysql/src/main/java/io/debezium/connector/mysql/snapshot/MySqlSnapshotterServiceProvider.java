/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class MySqlSnapshotterServiceProvider extends SnapshotterServiceProvider {

    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        MySqlConnectorConfig mySqlConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, MySqlConnectorConfig.class);

        return mySqlConnectorConfig.getSnapshotMode().getValue();
    }
}

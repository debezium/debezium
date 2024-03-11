/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class MongoDbSnapshotterServiceProvider extends SnapshotterServiceProvider {

    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        MongoDbConnectorConfig mySqlConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, MongoDbConnectorConfig.class);

        return mySqlConnectorConfig.getSnapshotMode().getValue();
    }
}
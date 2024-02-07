/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class OracleSnapshotterServiceProvider extends SnapshotterServiceProvider {

    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        OracleConnectorConfig mySqlConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, OracleConnectorConfig.class);

        return mySqlConnectorConfig.getSnapshotMode().getValue();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.snapshot.SnapshotterServiceProvider;

public class PostgresSnapshotterServiceProvider extends SnapshotterServiceProvider {
    @Override
    public String snapshotMode(BeanRegistry beanRegistry) {

        PostgresConnectorConfig postgresConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, PostgresConnectorConfig.class);

        return postgresConnectorConfig.snapshotMode().getValue();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures.connectors;

import io.debezium.testing.openshift.TestUtils;
import io.debezium.testing.openshift.fixtures.TestRuntimeFixture;
import io.debezium.testing.openshift.fixtures.TestSetupFixture;
import io.debezium.testing.openshift.resources.ConnectorFactories;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.debezium.testing.openshift.tools.kafka.ConnectorConfigBuilder;

public interface PostgreSqlConnector
        extends TestSetupFixture, ConnectorSetupFixture, TestRuntimeFixture<SqlDatabaseController> {

    String CONNECTOR_NAME = "inventory-connector-postgresql";

    @Override
    default void setupConnector() throws Exception {
        String connectorName = CONNECTOR_NAME + "-" + TestUtils.getUniqueId();
        ConnectorConfigBuilder connectorConfig = new ConnectorFactories().postgresql(connectorName);
        decorateConnectorConfig(connectorConfig);

        setConnectorConfig(connectorConfig);

        getKafkaConnectController().deployConnector(connectorConfig);
    }

    @Override
    default void teardownConnector() throws Exception {
        getKafkaConnectController().undeployConnector(getConnectorConfig().getDbServerName());
    }
}

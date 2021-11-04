/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.connectors;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.fixtures.TestRuntimeFixture;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

public interface OracleConnector
        extends TestSetupFixture, ConnectorSetupFixture, TestRuntimeFixture<SqlDatabaseController> {

    String CONNECTOR_NAME = "inventory-connector-oracle";

    @Override
    default void setupConnector() throws Exception {
        String connectorName = CONNECTOR_NAME + "-" + TestUtils.getUniqueId();
        ConnectorConfigBuilder connectorConfig = new ConnectorFactories(getKafkaController()).oracle(getDbController(), connectorName);
        decorateConnectorConfig(connectorConfig);

        setConnectorConfig(connectorConfig);

        getKafkaConnectController().deployConnector(connectorConfig);
    }

    @Override
    default void teardownConnector() throws Exception {
        getKafkaConnectController().undeployConnector(getConnectorConfig().getConnectorName());
    }
}

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
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

public interface MongoConnector
        extends TestSetupFixture, ConnectorSetupFixture, TestRuntimeFixture<MongoDatabaseController> {

    String CONNECTOR_NAME = "inventory-connector-mongo";

    @Override
    default void setupConnector() throws Exception {
        String connectorName = CONNECTOR_NAME + "-" + TestUtils.getUniqueId();
        ConnectorConfigBuilder connectorConfig = new ConnectorFactories(getKafkaController()).mongo(getDbController(), connectorName);
        decorateConnectorConfig(connectorConfig);

        setConnectorConfig(connectorConfig);

        getKafkaConnectController().deployConnector(connectorConfig);
    }

    @Override
    default void teardownConnector() throws Exception {
        getKafkaConnectController().undeployConnector(getConnectorConfig().getConnectorName());
    }
}

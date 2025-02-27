/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.connectors;

import java.io.IOException;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.tools.databases.DatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.registry.RegistryController;

import fixture5.TestFixture;

public abstract class ConnectorFixture<T extends DatabaseController<?>> extends TestFixture {

    private static final String CONNECTOR_NAME = "inventory-connector-mysql";

    protected final KafkaController kafkaController;
    protected final KafkaConnectController connectController;
    protected final RegistryController apicurioController;
    protected final Class<T> controllerType;
    protected final T dbController;
    protected final String connectorBaseName;
    protected ConnectorConfigBuilder connectorConfig;

    public ConnectorFixture(String connectorName, Class<T> controllerType, ExtensionContext.Store store) {
        super(store);

        this.connectorBaseName = connectorName;
        this.controllerType = controllerType;
        this.kafkaController = retrieve(KafkaController.class);
        this.connectController = retrieve(KafkaConnectController.class);
        this.dbController = retrieve(controllerType);
        this.apicurioController = retrieve(RegistryController.class);
    }

    protected abstract ConnectorConfigBuilder connectorConfig(String connectorName);

    @Override
    public void setup() throws Exception {
        String connectorName = connectorBaseName + TestUtils.getUniqueId();
        connectorConfig = connectorConfig(connectorName);
        addApicurioConfig();
        connectController.deployConnector(connectorConfig);
        store(ConnectorConfigBuilder.class, connectorConfig);
    }

    protected void addApicurioConfig() {
        if (apicurioController != null) {
            connectorConfig.addApicurioAvroSupport(apicurioController.getRegistryApiAddress());
        }
    }

    @Override
    public void teardown() throws IOException {
        if (connectorConfig != null) {
            connectController.undeployConnector(connectorConfig.getConnectorName());
        }
    }
}

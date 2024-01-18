/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.connectors;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { KafkaController.class, KafkaConnectController.class, OcpMongoShardedController.class }, provides = { ConnectorConfigBuilder.class })
public class ShardedReplicaMongoConnector extends ConnectorFixture<OcpMongoShardedController> {
    private static final String CONNECTOR_NAME = "inventory-connector-mongo";

    public ShardedReplicaMongoConnector(ExtensionContext.Store store) {
        super(CONNECTOR_NAME, OcpMongoShardedController.class, store);
    }

    @Override
    public ConnectorConfigBuilder connectorConfig(String connectorName) {
        return new ConnectorFactories(kafkaController).shardedReplicaMongo(dbController, connectorName);
    }
}

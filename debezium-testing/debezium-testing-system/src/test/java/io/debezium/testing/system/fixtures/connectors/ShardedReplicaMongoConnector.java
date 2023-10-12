package io.debezium.testing.system.fixtures.connectors;

import fixture5.annotations.FixtureContext;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import org.junit.jupiter.api.extension.ExtensionContext;

@FixtureContext(requires = { KafkaController.class, KafkaConnectController.class, MongoDatabaseController.class }, provides = { ConnectorConfigBuilder.class })
public class ShardedReplicaMongoConnector extends ConnectorFixture<MongoDatabaseController> {
    private static final String CONNECTOR_NAME = "inventory-connector-mongo";

    public ShardedReplicaMongoConnector(ExtensionContext.Store store) {
        super(CONNECTOR_NAME, MongoDatabaseController.class, store);
    }

    @Override
    public ConnectorConfigBuilder connectorConfig(String connectorName) {
        return new ConnectorFactories(kafkaController).shardedReplicaMongo(dbController, connectorName);
    }
}

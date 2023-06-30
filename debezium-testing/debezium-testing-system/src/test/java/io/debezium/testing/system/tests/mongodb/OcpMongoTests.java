/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mongodb;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;

import java.io.IOException;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public abstract class OcpMongoTests extends MongoTests {
    public OcpMongoTests(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                         KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @Test
    @Order(100)
    public void shouldStreamInShardedMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        connectController.undeployConnector(connectorConfig.getConnectorName());

        String connectorName = "sharded-connector" + TestUtils.getUniqueId();
        connectorConfig = new ConnectorFactories(kafkaController)
                .mongo(dbController, connectorName)
                .put("mongodb.connection.string", "mongodb://" + dbController.getDatabaseHostname() + ":" + dbController.getDatabasePort())
                .put("mongodb.connection.mode", "sharded")
                .put("topic.prefix", connectorName);
        connectController.deployConnector(connectorConfig);
        insertCustomer(dbController, "Adam", "Sharded", "ashard@test.com");

        String topic = connectorName + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ashard@test.com"));
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));

        insertProduct(dbController, "Sharded_mode", "aaa", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorName + ".inventory.products", "Sharded_mode"));

        addAndRemoveShardTest(dbController, connectorName);
        connectController.undeployConnector(connectorConfig.getConnectorName());
    }

    @Test
    @Order(110)
    public void shouldStreamInReplicaSetMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        String connectorName = "replicaset-connector" + TestUtils.getUniqueId();
        connectorConfig = new ConnectorFactories(kafkaController)
                .mongo(dbController, connectorName)
                .put("mongodb.connection.string", "mongodb://" + dbController.getDatabaseHostname() + ":" + dbController.getDatabasePort())
                .put("mongodb.connection.mode", "replica_set")
                .put("task.max", 4)
                .put("topic.prefix", connectorName);
        connectController.deployConnector(connectorConfig);
        String topic = connectorName + ".inventory.customers";
        assertions.assertTopicsExist(
                connectorName + ".inventory.customers");

        insertCustomer(dbController, "Eve", "Sharded", "eshard@test.com");

        awaitAssert(() -> assertions.assertRecordsContain(topic, "eshard@test.com"));

        insertProduct(dbController, "Replicaset_mode", "aaa", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorName + ".inventory.products", "Replicaset_mode"));

        addAndRemoveShardTest(dbController, connectorName);
        connectController.undeployConnector(connectorName);
    }

    private void addAndRemoveShardTest(OcpMongoShardedController dbController, String connectorName) throws IOException, InterruptedException {
        String topic = connectorName + ".inventory.customers";

        // add shard, restart connector, insert to that shard and verify that insert was captured by debezium
        dbController.addShard(OcpMongoShardedController.MongoComponents.SHARD3R1);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);

        insertCustomer(dbController, "F", "F", "f@test.com");

        awaitAssert(() -> assertions.assertRecordsContain(topic, "f@test.com"));

        // remove shard, restart connector and verify debezium is still streaming
        removeCustomer(dbController, "f@test.com");
        dbController.removeShard(OcpMongoShardedController.MongoComponents.SHARD3R1);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);
        insertCustomer(dbController, "David", "Duck", "duck@test.com");

        awaitAssert(() -> assertions.assertRecordsContain(topic, "duck@test.com"));
    }
}

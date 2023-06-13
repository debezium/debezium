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

        // insertProduct
        // assert
        connectController.undeployConnector(connectorConfig.getConnectorName());
    }

    @Test
    @Order(110)
    public void shouldStreamInReplicaSetMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        String connectorName = "replicaset-connector" + TestUtils.getUniqueId();
        connectorConfig = new ConnectorFactories(kafkaController)
                .mongo(dbController, connectorName)
                 .put("mongodb.connection.string", dbController.getDatabaseHostname() + ":" + dbController.getDatabasePort())
//                .put("mongodb.connection.string", "mongodb://" + dbController.getDatabaseHostname() + ":" + dbController.getDatabasePort() + "/?replicaSet=rs0")
                .put("mongodb.connection.mode", "replica_set")
                .put("task.max", 4)
                .put("topic.prefix", connectorName);
        connectorConfig.get().remove("mongodb.hosts");

        connectController.deployConnector(connectorConfig);

        String topic = connectorName + ".inventory.customers";

        insertCustomer(dbController, "Eve", "Sharded", "eshard@test.com");

        String prefix = connectorName;
        assertions.assertTopicsExist(
                prefix + ".inventory.customers");
        awaitAssert(() -> assertions.assertRecordsContain(topic, "eshard@test.com"));
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));

        // assert

        // insertProduct
        // assert
        connectController.undeployConnector(connectorConfig.getConnectorName());
    }

    @Test
    @Order(120)
    public void addShard() {
        // add shard
        // edit connector
        // insert
        // assert

        // insertProduct
        // assert
    }

    @Test
    @Order(130)
    public void removeShard() {
        // TODO when can I remove shard?
        // remove shard
        // edit connector
        // insert
        // assert

        // insertProduct
        // assert
    }
}

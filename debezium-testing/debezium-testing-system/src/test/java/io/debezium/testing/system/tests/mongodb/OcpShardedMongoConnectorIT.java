/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_LOGIN_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_USERNAME;

import java.io.IOException;

import io.debezium.testing.system.fixtures.databases.ocp.OcpMongo;
import io.debezium.testing.system.fixtures.databases.ocp.OcpMongoSharded;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.connectors.MongoConnector;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.fixtures.operator.OcpStrimziOperator;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mongo")
@Tag("openshift")
@Tag("mongo-sharded")
@Fixture(OcpClient.class)
@Fixture(OcpStrimziOperator.class)
@Fixture(OcpKafka.class)
@Fixture(OcpMongoSharded.class)
@Fixture(MongoConnector.class)
@ExtendWith(FixtureExtension.class)
public abstract class OcpShardedMongoConnectorIT extends ConnectorTest {
    public OcpShardedMongoConnectorIT(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                                      KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    public void insertCustomer(MongoDatabaseController dbController, String firstName, String lastName, String email, long id) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            Document doc = new Document()
                    .append("_id", id)
                    .append("first_name", firstName)
                    .append("last_name", lastName)
                    .append("email", email);
            col.insertOne(doc);
        });
    }

    public void removeCustomer(MongoDatabaseController dbController, String email) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute("inventory", "customers", col -> {
            Bson query = eq("email", email);
            col.deleteOne(col.find(query).first());
        });
    }

    public void insertProduct(MongoDatabaseController dbController, String name, String description, String weight, int quantity) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "products", col -> {
            Document doc = new Document()
                    .append("name", name)
                    .append("description", description)
                    .append("weight", weight)
                    .append("quantity", quantity);
            col.insertOne(doc);
        });
    }

    private void addAndRemoveShardTest(OcpMongoShardedController dbController, String connectorName) throws IOException, InterruptedException {
        String topic = connectorName + ".inventory.customers";
        int rangeStart = 1100;
        int rangeEnd = 1105;

        // add shard, restart connector, insert to that shard and verify that insert was captured by debezium
        dbController.addShard(3, "THREE", rangeStart, rangeEnd);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);

        insertCustomer(dbController, "Filip", "Foobar", "ffoo@test.com", 1101);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "ffoo@test.com"));

        // remove shard, restart connector and verify debezium is still streaming
        removeCustomer(dbController, "ffoo@test.com");
        dbController.removeShard(3, rangeStart, rangeEnd);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);
    }

    @Test
    @Order(100)
    public void shouldStreamInShardedMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        connectController.undeployConnector(connectorConfig.getConnectorName());

        String connectorName = "sharded-connector" + TestUtils.getUniqueId();
        connectorConfig = new ConnectorFactories(kafkaController)
                .mongo(dbController, connectorName)
                .put("mongodb.connection.string", dbController.getPublicDatabaseUrl())
                .put("mongodb.connection.mode", "sharded")
                .put("topic.prefix", connectorName);
        connectController.deployConnector(connectorConfig);
        insertCustomer(dbController, "Adam", "Sharded", "ashard@test.com", 1005);

        String topic = connectorName + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ashard@test.com"));
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));

        insertProduct(dbController, "sharded product", "demonstrates, that sharded connector mode works", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorName + ".inventory.products", "sharded product"));

        addAndRemoveShardTest(dbController, connectorName);

        insertCustomer(dbController, "David", "Duck", "duck@test.com", 1006);
        awaitAssert(() -> assertions.assertRecordsContain(topic, "duck@test.com"));

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

        insertCustomer(dbController, "Eve", "Sharded", "eshard@test.com", 1007);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "eshard@test.com"));
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 7));

        insertProduct(dbController, "replicaset product", "demonstrates that replicaset connector mode works", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorName + ".inventory.products", "replicaset product"));

        addAndRemoveShardTest(dbController, connectorName);

        insertCustomer(dbController, "Eric", "Eh", "ee@test.com", 1008);
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ee@test.com"));
        connectController.undeployConnector(connectorName);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mongodb.sharded;

import static com.mongodb.client.model.Filters.eq;
import static io.debezium.testing.system.assertions.KafkaAssertions.LOGGER;
import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_SA_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_USERNAME;

import java.io.IOException;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.sharded.MongoShardedUtil;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoShardedController;
import io.debezium.testing.system.tools.databases.mongodb.sharded.ShardKeyRange;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentfactories.OcpShardModelProvider;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import freemarker.template.TemplateException;

public abstract class ShardedMongoTests extends ConnectorTest {
    public ShardedMongoTests(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                             KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    public void insertCustomer(MongoDatabaseController dbController, String firstName, String lastName, String email, long id) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_USERNAME, DATABASE_MONGO_SA_PASSWORD);
        LOGGER.info("Creating customer: " + email);

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
                .getDatabaseClient(DATABASE_MONGO_USERNAME, DATABASE_MONGO_SA_PASSWORD);

        client.execute("inventory", "customers", col -> {
            Bson query = eq("email", email);
            col.deleteOne(col.find(query).first());
        });
    }

    public void removeProduct(MongoDatabaseController dbController, String name) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_USERNAME, DATABASE_MONGO_SA_PASSWORD);
        client.execute("inventory", "products", col -> {
            Bson query = eq("name", name);
            col.deleteOne(col.find(query).first());
        });
    }

    public void insertProduct(MongoDatabaseController dbController, String name, String description, String weight, int quantity) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_USERNAME, DATABASE_MONGO_SA_PASSWORD);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "products", col -> {
            Document doc = new Document()
                    .append("name", name)
                    .append("description", description)
                    .append("weight", weight)
                    .append("quantity", quantity);
            col.insertOne(doc);
        });
    }

    protected void addAndRemoveShardTest(OcpMongoShardedController dbController, String connectorName) throws IOException, InterruptedException, TemplateException {
        String topic = connectorName + ".inventory.customers";

        // add shard, restart connector, insert to that shard and verify that insert was captured by debezium
        var key = dbController.getMongo().getShardKey("inventory.customers");
        var keyRange = new ShardKeyRange(OcpShardModelProvider.getShardReplicaSetName(3), "1100", "1105");
        dbController.addShard(Map.of(key, keyRange));
        var sets = dbController.getMongo().getShardReplicaSets();
        sets.get(sets.size() - 1).executeMongosh(
                MongoShardedUtil.createDebeziumUserCommand(ConfigProperties.DATABASE_MONGO_DBZ_USERNAME, ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD), true);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);

        insertCustomer(dbController, "Filip", "Foobar", "ffoo@test.com", 1101);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "ffoo@test.com"));

        // remove shard, restart connector and verify debezium is still streaming
        removeCustomer(dbController, "ffoo@test.com");
        dbController.removeShard();

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);
    }
}

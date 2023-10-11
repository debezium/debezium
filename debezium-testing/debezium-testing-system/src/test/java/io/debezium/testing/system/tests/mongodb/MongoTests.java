/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mongodb;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_LOGIN_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.bson.Document;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public abstract class MongoTests extends ConnectorTest {

    public MongoTests(
                      KafkaController kafkaController,
                      KafkaConnectController connectController,
                      ConnectorConfigBuilder connectorConfig,
                      KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    public void insertCustomer(MongoDatabaseController dbController, String firstName, String lastName, String email) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            Document doc = new Document()
                    .append("first_name", firstName)
                    .append("last_name", lastName)
                    .append("email", email);
            col.insertOne(doc);
        });
    }

    public void renameCustomer(MongoDatabaseController dbController, String oldName, String newName) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            col.updateOne(Filters.eq("first_name", oldName), Updates.set("first_name", newName));
        });
    }

    @Test
    @Order(10)
    public void shouldHaveRegisteredConnector() {

        Request r = new Request.Builder().url(connectController.getApiURL().resolve("/connectors")).build();

        awaitAssert(() -> {
            try (Response res = new OkHttpClient().newCall(r).execute()) {
                assertThat(res.body().string()).contains(connectorConfig.getConnectorName());
            }
        });
    }

    @Test
    @Order(20)
    public void shouldCreateKafkaTopics() {
        String prefix = connectorConfig.getDbServerName();
        assertions.assertTopicsExist(
                prefix + ".inventory.customers",
                prefix + ".inventory.orders",
                prefix + ".inventory.products");
    }

    @Test
    @Order(30)
    public void shouldSnapshotChanges() {
        connectController.getMetricsReader().waitForMongoSnapshot(connectorConfig.getDbServerName());

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 4));
    }

    @Test
    @Order(40)
    public void shouldStreamChanges(MongoDatabaseController dbController) throws SQLException {
        insertCustomer(dbController, "Tom", "Tester", "tom@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "tom@test.com"));
    }

    @Test
    @Order(41)
    public void shouldRerouteUpdates(MongoDatabaseController dbController) throws SQLException {
        renameCustomer(dbController, "Tom", "Thomas");

        String prefix = connectorConfig.getDbServerName();
        String updatesTopic = prefix + ".u.customers";
        awaitAssert(() -> assertions.assertRecordsCount(prefix + ".inventory.customers", 5));
        awaitAssert(() -> assertions.assertRecordsCount(updatesTopic, 1));
        awaitAssert(() -> assertions.assertRecordsContain(updatesTopic, "Thomas"));
    }

    @Test
    @Order(50)
    public void shouldBeDown(MongoDatabaseController dbController) throws Exception {
        connectController.undeployConnector(connectorConfig.getConnectorName());
        insertCustomer(dbController, "Jerry", "Tester", "jerry@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
    }

    @Test
    @Order(60)
    public void shouldResumeStreamingAfterRedeployment() throws Exception {
        connectController.deployConnector(connectorConfig);

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 6));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "jerry@test.com"));
    }

    @Test
    @Order(70)
    public void shouldBeDownAfterCrash(MongoDatabaseController dbController) throws SQLException {
        connectController.destroy();
        insertCustomer(dbController, "Nibbles", "Tester", "nibbles@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 6));
    }

    @Test
    @Order(80)
    public void shouldResumeStreamingAfterCrash() throws InterruptedException {
        connectController.restore();

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 7));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "nibbles@test.com"));
    }
}

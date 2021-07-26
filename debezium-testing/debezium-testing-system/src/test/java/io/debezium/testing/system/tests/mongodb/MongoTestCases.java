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

import java.io.IOException;

import org.bson.Document;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.fixtures.TestRuntimeFixture;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public interface MongoTestCases extends TestRuntimeFixture<MongoDatabaseController> {

    default void insertCustomer(String firstName, String lastName, String email) {
        MongoDatabaseClient client = getDbController().getDatabaseClient(
                DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);
        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            Document doc = new Document()
                    .append("first_name", firstName)
                    .append("last_name", lastName)
                    .append("email", email);
            col.insertOne(doc);
        });
    }

    @Test
    @Order(1)
    default void shouldHaveRegisteredConnector() {
        Request r = new Request.Builder()
                .url(getKafkaConnectController().getApiURL().resolve("/connectors"))
                .build();
        awaitAssert(() -> {
            try (Response res = new OkHttpClient().newCall(r).execute()) {
                assertThat(res.body().string()).contains(getConnectorConfig().getConnectorName());
            }
        });
    }

    @Test
    @Order(2)
    default void shouldCreateKafkaTopics() {
        String prefix = getConnectorConfig().getDbServerName();
        assertions().assertTopicsExist(
                prefix + ".inventory.customers",
                prefix + ".inventory.orders",
                prefix + ".inventory.products");
    }

    @Test
    @Order(3)
    default void shouldContainRecordsInCustomersTopic() throws IOException {
        getConnectorMetrics().waitForMongoSnapshot(getConnectorConfig().getDbServerName());

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 4));
    }

    @Test
    @Order(4)
    default void shouldStreamChanges() {
        insertCustomer("Tom", "Tester", "tom@test.com");

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions().assertRecordsContain(topic, "tom@test.com"));
    }

    @Test
    @Order(5)
    default void shouldBeDown() throws IOException {
        getKafkaConnectController().undeployConnector(getConnectorConfig().getConnectorName());
        insertCustomer("Jerry", "Tester", "jerry@test.com");

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 5));
    }

    @Test
    @Order(6)
    default void shouldResumeStreamingAfterRedeployment() throws IOException, InterruptedException {
        getKafkaConnectController().deployConnector(getConnectorConfig());

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 6));
        awaitAssert(() -> assertions().assertRecordsContain(topic, "jerry@test.com"));
    }

    @Test
    @Order(7)
    default void shouldBeDownAfterCrash() {
        getKafkaConnectController().destroy();

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        insertCustomer("Nibbles", "Tester", "nibbles@test.com");
        awaitAssert(() -> assertions().assertRecordsCount(topic, 6));
    }

    @Test
    @Order(8)
    default void shouldResumeStreamingAfterCrash() throws InterruptedException {
        getKafkaConnectController().restore();

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertMinimalRecordsCount(topic, 7));
        awaitAssert(() -> assertions().assertRecordsContain(topic, "nibbles@test.com"));
    }
}

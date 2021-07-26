/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mysql;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.fixtures.TestRuntimeFixture;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public interface MySqlTestCases extends TestRuntimeFixture<SqlDatabaseController> {

    default void insertCustomer(String firstName, String lastName, String email) throws SQLException {
        SqlDatabaseClient client = getDbController().getDatabaseClient(DATABASE_MYSQL_USERNAME, DATABASE_MYSQL_PASSWORD);
        String sql = "INSERT INTO customers VALUES  (default, '" + firstName + "', '" + lastName + "', '" + email + "')";
        client.execute("inventory", sql);
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
                prefix + ".inventory.addresses",
                prefix + ".inventory.customers",
                prefix + ".inventory.geom",
                prefix + ".inventory.orders",
                prefix + ".inventory.products",
                prefix + ".inventory.products_on_hand");
    }

    @Test
    @Order(3)
    default void shouldSnapshotChanges() throws IOException {
        getConnectorMetrics().waitForMySqlSnapshot(getConnectorConfig().getDbServerName());

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 4));
    }

    @Test
    @Order(4)
    default void shouldStreamChanges() throws SQLException {
        insertCustomer("Tom", "Tester", "tom@test.com");

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions().assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions().assertRecordsContain(topic, "tom@test.com"));
    }

    @Test
    @Order(5)
    default void shouldBeDown() throws SQLException, IOException {
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
    default void shouldBeDownAfterCrash() throws SQLException {
        getKafkaConnectController().destroy();
        insertCustomer("Nibbles", "Tester", "nibbles@test.com");

        String topic = getConnectorConfig().getDbServerName() + ".inventory.customers";
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

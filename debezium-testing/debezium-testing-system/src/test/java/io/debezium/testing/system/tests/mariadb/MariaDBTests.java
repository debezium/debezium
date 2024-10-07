/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mariadb;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MARIADB_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MARIADB_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.mariadb.MariaDbController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public abstract class MariaDBTests extends ConnectorTest {

    public MariaDBTests(
                        KafkaController kafkaController,
                        KafkaConnectController connectController,
                        ConnectorConfigBuilder connectorConfig,
                        KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    public void insertCustomer(
                               SqlDatabaseController dbController,
                               String firstName, String lastName,
                               String email)
            throws SQLException {
        SqlDatabaseClient client = dbController.getDatabaseClient(DATABASE_MARIADB_USERNAME, DATABASE_MARIADB_PASSWORD);
        String sql = "INSERT INTO customers VALUES  (default, '" + firstName + "', '" + lastName + "', '" + email + "')";
        client.execute("inventory", sql);
    }

    public void renameCustomer(SqlDatabaseController dbController, String oldName, String newName) throws SQLException {
        SqlDatabaseClient client = dbController.getDatabaseClient(DATABASE_MARIADB_USERNAME, DATABASE_MARIADB_PASSWORD);
        String sql = "UPDATE customers SET first_name = '" + newName + "' WHERE first_name = '" + oldName + "'";
        client.execute("inventory", sql);
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
                prefix + ".inventory.addresses", prefix + ".inventory.customers", prefix + ".inventory.geom",
                prefix + ".inventory.orders", prefix + ".inventory.products", prefix + ".inventory.products_on_hand");
    }

    @Test
    @Order(30)
    public void shouldSnapshotChanges() {
        connectController.getMetricsReader().waitForMariaDbSnapshot(connectorConfig.getDbServerName());

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 4));
    }

    @Test
    @Order(40)
    public void shouldStreamChanges(MariaDbController dbController) throws SQLException {
        insertCustomer(dbController, "Tom", "Tester", "tom@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "tom@test.com"));
    }

    @Test
    @Order(41)
    public void shouldRerouteUpdates(MariaDbController dbController) throws SQLException {
        renameCustomer(dbController, "Tom", "Thomas");

        String prefix = connectorConfig.getDbServerName();
        String updatesTopic = prefix + ".u.customers";
        awaitAssert(() -> assertions.assertRecordsCount(prefix + ".inventory.customers", 5));
        awaitAssert(() -> assertions.assertRecordsCount(updatesTopic, 1));
        awaitAssert(() -> assertions.assertRecordsContain(updatesTopic, "Thomas"));
    }

    @Test
    @Order(50)
    public void shouldBeDown(MariaDbController dbController) throws Exception {
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
    public void shouldBeDownAfterCrash(MariaDbController dbController) throws SQLException {
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

    @Test
    @Order(90)
    public void shouldExtractNewRecordState(MariaDbController dbController) throws Exception {
        connectController.undeployConnector(connectorConfig.getConnectorName());
        connectorConfig = connectorConfig.addJdbcUnwrapSMT();
        connectController.deployConnector(connectorConfig);

        insertCustomer(dbController, "Eaton", "Beaver", "ebeaver@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 8));
        awaitAssert(() -> assertions.assertRecordIsUnwrapped(topic, 1));
    }
}

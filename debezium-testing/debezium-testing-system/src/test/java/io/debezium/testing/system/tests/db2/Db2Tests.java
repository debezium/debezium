/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.db2;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public abstract class Db2Tests extends ConnectorTest {

    public Db2Tests(
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
        SqlDatabaseClient client = dbController.getDatabaseClient(DATABASE_MYSQL_USERNAME, DATABASE_MYSQL_PASSWORD);
        String sql = "INSERT INTO DB2INST1.CUSTOMERS(first_name,last_name,email) VALUES  ('" + firstName + "', '" + lastName + "', '" + email + "')";
        client.execute("inventory", sql);
    }

    @Test
    @Order(1)
    public void shouldHaveRegisteredConnector() {

        Request r = new Request.Builder().url(connectController.getApiURL().resolve("/connectors")).build();

        awaitAssert(() -> {
            try (Response res = new OkHttpClient().newCall(r).execute()) {
                assertThat(res.body().string()).contains(connectorConfig.getConnectorName());
            }
        });
    }

    @Test
    @Order(2)
    public void shouldCreateKafkaTopics() {
        String prefix = connectorConfig.getDbServerName();
        assertions.assertTopicsExist(
                prefix + ".DB2INST1.CUSTOMERS",
                prefix + ".DB2INST1.ORDERS",
                prefix + ".DB2INST1.PRODUCTS",
                prefix + ".DB2INST1.PRODUCTS_ON_HAND");
    }

    @Test
    @Order(3)
    public void shouldSnapshotChanges() {
        connectController.getMetricsReader().waitForDB2Snapshot(connectorConfig.getDbServerName());

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 4));
    }

    @Test
    @Order(4)
    public void shouldStreamChanges(SqlDatabaseController dbController) throws SQLException {
        insertCustomer(dbController, "Tom", "Tester", "tom@test.com");

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "tom@test.com"));
    }

    @Test
    @Order(5)
    public void shouldBeDown(SqlDatabaseController dbController) throws Exception {
        connectController.undeployConnector(connectorConfig.getConnectorName());
        insertCustomer(dbController, "Jerry", "Tester", "jerry@test.com");

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
    }

    @Test
    @Order(6)
    public void shouldResumeStreamingAfterRedeployment() throws Exception {
        connectController.deployConnector(connectorConfig);

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 6));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "jerry@test.com"));
    }

    @Test
    @Order(7)
    public void shouldBeDownAfterCrash(SqlDatabaseController dbController) throws SQLException {
        connectController.destroy();
        insertCustomer(dbController, "Nibbles", "Tester", "nibbles@test.com");

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 6));
    }

    @Test
    @Order(8)
    public void shouldResumeStreamingAfterCrash() throws InterruptedException {
        connectController.restore();

        String topic = connectorConfig.getDbServerName() + ".DB2INST1.CUSTOMERS";
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 7));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "nibbles@test.com"));
    }
}

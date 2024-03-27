/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mysql;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tools.databases.mysql.MySqlController;
import io.debezium.testing.system.tools.databases.mysql.MySqlReplicaController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public abstract class MySqlOcpTests extends MySqlTests {
    public MySqlOcpTests(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                         KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @Test
    @Order(100)
    public void shouldStreamFromReplica(MySqlReplicaController replicaController, MySqlController masterController)
            throws InterruptedException, IOException, SQLException {
        await()
                .atMost(scaled(5), TimeUnit.MINUTES)
                .pollInterval(Duration.ofSeconds(20))
                .until(() -> Objects.equals(getCustomerCount(replicaController), getCustomerCount(masterController)));

        connectorConfig.put("database.hostname", replicaController.getDatabaseHostname());
        connectController.deployConnector(connectorConfig);

        insertCustomer(masterController, "Arnold", "Test", "atest@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 9));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "atest@test.com"));
    }

    @Test
    @Order(110)
    public void shouldStreamAfterMasterRestart(MySqlReplicaController replicaController, MySqlController masterController)
            throws SQLException, IOException, InterruptedException {
        connectorConfig.put("database.hostname", masterController.getDatabaseHostname());
        connectController.deployConnector(connectorConfig);

        insertCustomer(masterController, "Alex", "master", "amaster@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 10));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "amaster@test.com"));

        // restart only master after replication is complete, otherwise there is danger of duplicates in kafka topics
        await()
                .atMost(scaled(5), TimeUnit.MINUTES)
                .pollInterval(Duration.ofSeconds(20))
                .until(() -> Objects.equals(getCustomerCount(replicaController), getCustomerCount(masterController)));

        masterController.reload();

        connectorConfig.put("database.hostname", replicaController.getDatabaseHostname());
        connectController.deployConnector(connectorConfig);

        insertCustomer(masterController, "Tom", "Train", "ttrain@test.com");

        awaitAssert(() -> assertions.assertRecordsCount(topic, 11));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ttrain@test.com"));
    }
}

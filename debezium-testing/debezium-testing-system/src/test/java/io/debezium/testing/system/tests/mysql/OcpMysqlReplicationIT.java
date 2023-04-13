/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mysql;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tests.mysql.MySqlTests.getCustomerCount;
import static io.debezium.testing.system.tests.mysql.MySqlTests.insertCustomer;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.connectors.MySqlConnector;
import io.debezium.testing.system.fixtures.databases.ocp.OcpMySqlMaster;
import io.debezium.testing.system.fixtures.databases.ocp.OcpMySqlReplica;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.fixtures.operator.OcpStrimziOperator;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.mysql.MySqlMasterController;
import io.debezium.testing.system.tools.databases.mysql.MySqlReplicaController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mysql")
@Tag("openshift")
@Fixture(OcpClient.class)
@Fixture(OcpStrimziOperator.class)
@Fixture(OcpKafka.class)
@Fixture(OcpMySqlMaster.class)
@Fixture(OcpMySqlReplica.class)
@Fixture(MySqlConnector.class)
@ExtendWith(FixtureExtension.class)
public class OcpMysqlReplicationIT extends ConnectorTest {
    public OcpMysqlReplicationIT(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                                 KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @Test
    @Order(10)
    public void shouldStreamFromReplica(MySqlReplicaController replicaController, MySqlMasterController masterController)
            throws InterruptedException, IOException, SQLException {
        await()
                .atMost(scaled(5), TimeUnit.MINUTES)
                .pollInterval(Duration.ofSeconds(20))
                .until(() -> Objects.equals(getCustomerCount(replicaController), getCustomerCount(masterController)));

        connectorConfig.put("database.hostname", replicaController.getDatabaseHostname());
        connectController.deployConnector(connectorConfig);

        insertCustomer(masterController, "Arnold", "Test", "atest@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 5));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "atest@test.com"));
    }

    @Test
    @Order(20)
    public void shouldStreamAfterMasterRestart(MySqlReplicaController replicaController, MySqlMasterController masterController)
            throws SQLException, IOException, InterruptedException {
        connectorConfig.put("database.hostname", masterController.getDatabaseHostname());
        connectController.deployConnector(connectorConfig);

        insertCustomer(masterController, "Alex", "master", "amaster@test.com");

        String topic = connectorConfig.getDbServerName() + ".inventory.customers";
        awaitAssert(() -> assertions.assertRecordsCount(topic, 6));
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

        awaitAssert(() -> assertions.assertRecordsCount(topic, 7));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ttrain@test.com"));
    }
}

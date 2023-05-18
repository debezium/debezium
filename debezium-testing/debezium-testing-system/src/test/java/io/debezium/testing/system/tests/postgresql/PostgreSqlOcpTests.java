/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.postgresql;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.postgresql.OcpPostgreSqlReplicaController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public abstract class PostgreSqlOcpTests extends PostgreSqlTests {
    public PostgreSqlOcpTests(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                              KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @Test
    @Order(100)
    public void shouldStreamFromReplica(OcpPostgreSqlReplicaController replicaController, SqlDatabaseController primaryController)
            throws InterruptedException, IOException, SQLException {

        connectController.undeployConnector(connectorConfig.getConnectorName());

        var connector = new ConnectorFactories(kafkaController).postgresql(replicaController, "replica-connector");
        connectController.deployConnector(connector);

        String topic = connector.getDbServerName() + ".inventory.customers";

        awaitAssert(() -> assertions.assertRecordsCount(topic, 7));

        insertCustomer(primaryController, "Arnold", "Test", "atest@test.com");

        awaitAssert(() -> assertions.assertRecordsCount(topic, 8));
        awaitAssert(() -> assertions.assertRecordsContain(topic, "atest@test.com"));
    }
}

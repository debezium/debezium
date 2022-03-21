/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.sqlserver;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.connectors.SqlServerConnector;
import io.debezium.testing.system.fixtures.databases.ocp.OcpSqlServer;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("sqlserver")
@Tag("openshift")
@Fixture(OcpClient.class)
@Fixture(OcpKafka.class)
@Fixture(OcpSqlServer.class)
@Fixture(SqlServerConnector.class)
@ExtendWith(FixtureExtension.class)
public class OcpSqlServerConnectorIT extends SqlServerTests {

    public OcpSqlServerConnectorIT(KafkaController kafkaController,
                                   KafkaConnectController connectController,
                                   ConnectorConfigBuilder connectorConfig,
                                   KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }
}

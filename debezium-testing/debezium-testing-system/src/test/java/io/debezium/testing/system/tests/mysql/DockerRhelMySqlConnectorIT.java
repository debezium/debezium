/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.mysql;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.connectors.MySqlConnector;
import io.debezium.testing.system.fixtures.databases.docker.DockerMySql;
import io.debezium.testing.system.fixtures.kafka.DockerKafka;
import io.debezium.testing.system.tools.databases.mysql.MySqlController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mysql")
@Tag("rhel")
@Tag("docker")
@Fixture(DockerNetwork.class)
@Fixture(DockerKafka.class)
@Fixture(DockerMySql.class)
@Fixture(MySqlConnector.class)
@ExtendWith(FixtureExtension.class)
public class DockerRhelMySqlConnectorIT extends MySqlTests {

    public DockerRhelMySqlConnectorIT(KafkaController kafkaController,
                                      KafkaConnectController connectController,
                                      ConnectorConfigBuilder connectorConfig,
                                      KafkaAssertions<?, ?> assertions,
                                      MySqlController dbController) {
        super(kafkaController, connectController, connectorConfig, assertions, dbController);
    }
}

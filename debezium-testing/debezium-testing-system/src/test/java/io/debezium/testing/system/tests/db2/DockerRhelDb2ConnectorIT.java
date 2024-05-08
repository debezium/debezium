/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.db2;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.connectors.Db2Connector;
import io.debezium.testing.system.fixtures.databases.docker.DockerDb2;
import io.debezium.testing.system.fixtures.kafka.DockerKafka;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("db2")
@Tag("rhel")
@Tag("docker")
@Fixture(DockerNetwork.class)
@Fixture(DockerKafka.class)
@Fixture(DockerDb2.class)
@Fixture(Db2Connector.class)
@ExtendWith(FixtureExtension.class)
public class DockerRhelDb2ConnectorIT extends Db2Tests {

    public DockerRhelDb2ConnectorIT(KafkaController kafkaController,
                                    KafkaConnectController connectController,
                                    ConnectorConfigBuilder connectorConfig,
                                    KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }
}

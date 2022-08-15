/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tools.databases.PortForwardableDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public class MongoConnectorTest extends ConnectorTest {
    public MongoConnectorTest(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                              KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @BeforeEach
    public void setUpPortForward(MongoDatabaseController dbController) {
        if (!isRunningFromOcp()) {
            ((PortForwardableDatabaseController) dbController).forwardDatabasePorts();
        }
    }

    @AfterEach
    public void closePortForward(MongoDatabaseController dbController) throws IOException {
        if (!isRunningFromOcp()) {
            ((PortForwardableDatabaseController) dbController).closeDatabasePortForwards();
        }
    }
}

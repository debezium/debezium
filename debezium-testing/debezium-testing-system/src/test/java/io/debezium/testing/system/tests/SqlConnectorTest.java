/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests;

import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

public class SqlConnectorTest extends ConnectorTest {
    public SqlConnectorTest(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                            KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }
}

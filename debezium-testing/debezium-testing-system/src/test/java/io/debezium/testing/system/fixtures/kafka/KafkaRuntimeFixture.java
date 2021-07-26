/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.kafka;

import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.connectors.ConnectorMetricsReader;

public interface KafkaRuntimeFixture {

    void setKafkaController(KafkaController controller);

    KafkaConnectController getKafkaConnectController();

    void setKafkaConnectController(KafkaConnectController controller);

    KafkaController getKafkaController();

    default ConnectorMetricsReader getConnectorMetrics() {
        return getKafkaConnectController().getMetricsReader();
    }
}

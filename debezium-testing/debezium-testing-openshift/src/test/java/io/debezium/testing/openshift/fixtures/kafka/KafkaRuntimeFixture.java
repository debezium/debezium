/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures.kafka;

import io.debezium.testing.openshift.tools.kafka.KafkaConnectController;
import io.debezium.testing.openshift.tools.kafka.KafkaController;

public interface KafkaRuntimeFixture {

    void setKafkaController(KafkaController controller);

    KafkaConnectController getKafkaConnectController();

    void setKafkaConnectController(KafkaConnectController controller);

    KafkaController getKafkaController();
}

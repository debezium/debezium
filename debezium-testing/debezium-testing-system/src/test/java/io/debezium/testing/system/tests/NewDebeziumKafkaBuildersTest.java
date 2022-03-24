/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_CRD_VERSION;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.kafka.builders.kafka.OcpKafkaBuilder;
import io.debezium.testing.system.tools.kafka.builders.kafkaconnect.OcpKafkaConnectBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;

public class NewDebeziumKafkaBuildersTest {
    @Test
    public void kafkaBuilderTest() {
        Assertions.assertThat(new OcpKafkaBuilder().withDefaults().build())
                .isEqualTo(YAML.from("src/test/resources/kafka-resources/" + STRIMZI_CRD_VERSION + "/010-kafka.yaml", Kafka.class));

    }

    @Test
    public void kafkaConnectBuilderTest() {
        OcpKafkaConnectBuilder builder1 = new OcpKafkaConnectBuilder();
        Assertions.assertThat(new OcpKafkaConnectBuilder().withNonKcBuildSetup().build())
                .isEqualTo(YAML.from("src/test/resources/kafka-resources/" + STRIMZI_CRD_VERSION + "/021-kafka-connect.yaml", KafkaConnect.class));
    }
}

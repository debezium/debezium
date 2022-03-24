/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders.kafkaconnect;

import static io.debezium.testing.system.tools.kafka.builders.OcpKafkaConstants.DEFAULT_API_VERSION;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectSpec;

/**
 * This class simplifies building of kafkaConnect by providing pre-made configurations for whole kafkaConnect or parts of its definition
 */
public class OcpKafkaConnectBuilderFactory {
    public static String DEFAULT_KAFKA_CONNECT_METADATA_NAME = "debezium-kafka-connect-cluster";
    public static String DEFAULT_KAFKA_CONNECT_VERSION = System.getProperty("version.strimzi.kafka");
    public static String DEFAULT_IMAGE = System.getProperty("image.kc");
    public static String DEFAULT_BOOSTRAP_SERVER = "debezium-kafka-cluster-kafka-bootstrap:9093";

    public static KafkaConnectBuilder createNonKcBuildSetup() {
        return new KafkaConnectBuilder()
                .withApiVersion(DEFAULT_API_VERSION)
                .withMetadata(getDefaultMeta())
                .withSpec(getNonKcSpec());
    }

    public static KafkaConnectBuilder createKcBuildSetup() {
        return new KafkaConnectBuilder()
                .withApiVersion(DEFAULT_API_VERSION)
                .withMetadata(getDefaultMeta())
                .withSpec(getKcSpec());
    }

    private static ObjectMeta getDefaultMeta() {
        return new ObjectMetaBuilder()
                .withName(DEFAULT_KAFKA_CONNECT_METADATA_NAME)
                .build();
    }

    private static KafkaConnectSpec getNonKcSpec() {
        return OcpKafkaConnectSpecBuilderFactory.createNonKcSetup()
                .build();
    }

    private static KafkaConnectSpec getKcSpec() {
        return OcpKafkaConnectSpecBuilderFactory.createKcSetup()
                .build();
    }
}

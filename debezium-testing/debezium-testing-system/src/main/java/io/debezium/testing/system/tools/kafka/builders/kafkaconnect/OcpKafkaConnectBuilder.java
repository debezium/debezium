/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders.kafkaconnect;

import static io.debezium.testing.system.tools.kafka.builders.OcpKafkaConstants.DEFAULT_API_VERSION;
import static io.debezium.testing.system.tools.kafka.builders.OcpKafkaConstants.DEFAULT_KIND;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;

/**
 * This class simplifies building of kafkaConnect by providing pre-made configurations for whole kafkaConnect or parts of its definition
 */
public class OcpKafkaConnectBuilder extends io.strimzi.api.kafka.model.KafkaConnectBuilder {
    public static String DEFAULT_KAFKA_CONNECT_METADATA_NAME = "debezium-kafka-connect-cluster";
    public static String DEFAULT_KAFKA_CONNECT_VERSION = "${version.strimzi.kafka}";
    public static String DEFAULT_IMAGE = "${image.kc}";
    public static String DEFAULT_BOOSTRAP_SERVER = "debezium-kafka-cluster-kafka-bootstrap:9093";

    public OcpKafkaConnectBuilder() {
    }

    public OcpKafkaConnectBuilder withNonKcBuildSetup() {
        return this
                .withDefaultApiVersion()
                .withDefaultKind()
                .withDefaultMeta()
                .withNonKcSpec();
    }

    public OcpKafkaConnectBuilder withKcBuildSetup() {
        return this
                .withDefaultApiVersion()
                .withDefaultKind()
                .withDefaultMeta()
                .withKcSpec();
    }

    public OcpKafkaConnectBuilder withDefaultApiVersion() {
        return (OcpKafkaConnectBuilder) this.withApiVersion(DEFAULT_API_VERSION);
    }

    public OcpKafkaConnectBuilder withDefaultKind() {
        return (OcpKafkaConnectBuilder) this.withKind(DEFAULT_KIND);
    }

    public OcpKafkaConnectBuilder withDefaultMeta() {
        return (OcpKafkaConnectBuilder) this.withMetadata(new ObjectMetaBuilder()
                .withName(DEFAULT_KAFKA_CONNECT_METADATA_NAME)
                .build());
    }

    public OcpKafkaConnectBuilder withNonKcSpec() {
        return (OcpKafkaConnectBuilder) this.withSpec(new OcpKafkaConnectSpecBuilder()
                .withNonKcSetup()
                .build());
    }

    public OcpKafkaConnectBuilder withKcSpec() {
        return (OcpKafkaConnectBuilder) this.withSpec(new OcpKafkaConnectSpecBuilder()
                .withKcSetup()
                .build());
    }
}

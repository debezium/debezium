/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders.kafka;

import static io.debezium.testing.system.tools.kafka.builders.OcpKafkaConstants.DEFAULT_API_VERSION;
import static io.debezium.testing.system.tools.kafka.builders.OcpKafkaConstants.DEFAULT_KIND;

import java.util.List;
import java.util.Map;

import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaSpecBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.ZookeeperClusterSpecBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;

/**
 * This class simplifies building of kafka by providing default configuration for whole kafka or parts of its definition
 */
public class OcpKafkaBuilder extends KafkaBuilder {
    public static String DEFAULT_KAFKA_METADATA_NAME = "debezium-kafka-cluster";
    public static String DEFAULT_KAFKA_VERSION = "${version.strimzi.kafka}";

    public OcpKafkaBuilder() {
    }

    public OcpKafkaBuilder withDefaults() {
        return this
                .withDefaultApiVersion()
                .withDefaultKind()
                .withDefaultMeta()
                .withDefaultKafkaSpec();
    }

    public OcpKafkaBuilder withDefaultApiVersion() {
        return (OcpKafkaBuilder) this.withApiVersion(DEFAULT_API_VERSION);
    }

    public OcpKafkaBuilder withDefaultKind() {
        return (OcpKafkaBuilder) this.withKind(DEFAULT_KIND);
    }

    public OcpKafkaBuilder withDefaultMeta() {
        return (OcpKafkaBuilder) this.withMetadata(new ObjectMetaBuilder()
                .withName(DEFAULT_KAFKA_METADATA_NAME)
                .withGeneration(4L)
                .build());
    }

    public OcpKafkaBuilder withDefaultKafkaSpec() {
        return (OcpKafkaBuilder) this.withSpec(new KafkaSpecBuilder()
                .withEntityOperator(getDefaultEntityOperatorSpec())
                .withKafka(getDefaultKafkaClusterSpec())
                .withZookeeper(getDefaultZookeeper())
                .build());
    }

    private static EntityOperatorSpec getDefaultEntityOperatorSpec() {
        return new EntityOperatorSpecBuilder()
                .withTopicOperator(new EntityTopicOperatorSpec())
                .withUserOperator(new EntityUserOperatorSpec())
                .build();
    }

    private static List<GenericKafkaListener> getDefaultListeners() {
        return ImmutableList.of(new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .build(),
                new GenericKafkaListenerBuilder()
                        .withName("tls")
                        .withPort(9093)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .build(),
                new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .build());
    }

    private static Map<String, Object> getDefaultConfig() {
        return ImmutableMap.of("offsets.topic.replication.factor", 1,
                "transaction.state.log.replication.factor", 1,
                "transaction.state.log.min.isr", 1);
    }

    private static KafkaClusterSpec getDefaultKafkaClusterSpec() {
        return new KafkaClusterSpecBuilder()
                .withConfig(getDefaultConfig())
                .withReplicas(1)
                .withVersion(DEFAULT_KAFKA_VERSION)
                .withListeners(getDefaultListeners())
                .withStorage(new EphemeralStorage())
                .build();
    }

    private static ZookeeperClusterSpec getDefaultZookeeper() {
        return new ZookeeperClusterSpecBuilder()
                .withStorage(new EphemeralStorage())
                .withReplicas(1)
                .build();
    }

    public Kafka build() {
        Kafka buildable = new Kafka();
        buildable.setApiVersion(getApiVersion());
        buildable.setKind(getKind());
        buildable.setMetadata(getMetadata());
        buildable.setSpec(buildSpec());
        buildable.setStatus(getStatus());
        return buildable;
    }
}

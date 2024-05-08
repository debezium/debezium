/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_VERSION_KAFKA;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpecBuilder;

/**
 * This class simplifies building of kafka by providing default configuration for whole kafka or parts of its definition
 */
public final class FabricKafkaBuilder extends FabricBuilderWrapper<FabricKafkaBuilder, KafkaBuilder, Kafka> {
    public static String DEFAULT_KAFKA_NAME = "debezium-kafka-cluster";

    private FabricKafkaBuilder(KafkaBuilder kafkaBuilder) {
        super(kafkaBuilder);
    }

    @Override
    public Kafka build() {
        return builder.build();
    }

    public static FabricKafkaBuilder base() {
        KafkaClusterSpec kafka = defaultKafkaSpec();
        ZookeeperClusterSpec zookeeper = defaultKafkaZookeeperSpec();
        EntityOperatorSpec entityOperator = defaultKafkaEntityOperatorSpec();

        KafkaBuilder builder = new KafkaBuilder()
                .withNewMetadata()
                .withName(DEFAULT_KAFKA_NAME)
                .endMetadata()
                .withNewSpec()
                .withKafka(kafka)
                .withZookeeper(zookeeper)
                .withEntityOperator(entityOperator)
                .endSpec();

        return new FabricKafkaBuilder(builder);
    }

    public FabricKafkaBuilder withPullSecret(Optional<Secret> maybePullSecret) {
        maybePullSecret
                .map(s -> s.getMetadata().getName())
                .ifPresent(this::withPullSecret);
        return self();
    }

    public FabricKafkaBuilder withPullSecret(String pullSecretName) {
        PodTemplate podTemplate = new PodTemplateBuilder().addNewImagePullSecret(pullSecretName).build();

        builder
                .editSpec()
                .editKafka()
                .withNewTemplate().withPod(podTemplate).endTemplate()
                .endKafka()
                .editZookeeper()
                .withNewTemplate().withPod(podTemplate).endTemplate()
                .endZookeeper()
                .editEntityOperator()
                .withNewTemplate().withPod(podTemplate).endTemplate()
                .endEntityOperator()
                .endSpec();

        return self();
    }

    private static KafkaClusterSpec defaultKafkaSpec() {
        Map<String, Object> config = defaultKafkaConfig();
        List<GenericKafkaListener> listeners = defaultKafkaListeners();

        return new KafkaClusterSpecBuilder()
                .withConfig(config)
                .withReplicas(3)
                .withVersion(STRIMZI_VERSION_KAFKA)
                .withListeners(listeners)
                .withStorage(new EphemeralStorage())
                .build();
    }

    private static List<GenericKafkaListener> defaultKafkaListeners() {
        GenericKafkaListener plainInternal = new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .build();

        GenericKafkaListener tlsInternal = new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();

        GenericKafkaListener routeExternal = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withType(KafkaListenerType.ROUTE)
                .withTls(true)
                .build();

        return Arrays.asList(plainInternal, tlsInternal, routeExternal);
    }

    private static Map<String, Object> defaultKafkaConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("offsets.topic.replication.factor", 1);
        config.put("transaction.state.log.replication.factor", 1);
        config.put("transaction.state.log.min.isr", 1);

        return config;
    }

    private static ZookeeperClusterSpec defaultKafkaZookeeperSpec() {
        return new ZookeeperClusterSpecBuilder()
                .withStorage(new EphemeralStorage())
                .withReplicas(1)
                .build();
    }

    private static EntityOperatorSpec defaultKafkaEntityOperatorSpec() {
        EntityTopicOperatorSpec topicOperator = new EntityTopicOperatorSpec();
        EntityUserOperatorSpec userOperator = new EntityUserOperatorSpec();

        return new EntityOperatorSpecBuilder()
                .withTopicOperator(topicOperator)
                .withUserOperator(userOperator)
                .build();
    }
}

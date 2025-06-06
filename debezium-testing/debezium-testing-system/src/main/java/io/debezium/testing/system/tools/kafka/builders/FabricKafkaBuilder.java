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

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
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
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpecBuilder;

/**
 * This class simplifies building of kafka by providing default configuration for whole kafka or parts of its definition
 */
public final class FabricKafkaBuilder extends FabricBuilderWrapper<FabricKafkaBuilder, KafkaBuilder, Kafka> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FabricKafkaBuilder.class);
    private static Boolean USE_KRAFT;
    public static String DEFAULT_KAFKA_NAME = "debezium-kafka-cluster";
    public static String DEFAULT_NODE_POOL_NAME = "node-pool";

    private FabricKafkaBuilder(KafkaBuilder kafkaBuilder) {
        super(kafkaBuilder);
    }

    @Override
    public Kafka build() {
        return builder.build();
    }

    public static FabricKafkaBuilder base() {
        KafkaClusterSpec kafka = defaultKafkaSpec();
        EntityOperatorSpec entityOperator = defaultKafkaEntityOperatorSpec();

        KafkaBuilder builder = new KafkaBuilder()
                .withNewMetadata()
                .withName(DEFAULT_KAFKA_NAME)
                .endMetadata()
                .withNewSpec()
                .withKafka(kafka)
                .withEntityOperator(entityOperator)
                .endSpec();

        if (shouldKRaftBeUsed()) {
            Map<String, String> clusterAnnotations = Map.of("strimzi.io/node-pools", "enabled", "strimzi.io/kraft", "enabled");
            builder
                    .editMetadata()
                    .withAnnotations(clusterAnnotations)
                    .endMetadata()
                    .editSpec()
                    .withZookeeper(null)
                    .endSpec();
        }
        else {
            builder
                    .editSpec()
                    .withZookeeper(defaultKafkaZookeeperSpec())
                    .endSpec();
        }

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
                .editEntityOperator()
                .withNewTemplate().withPod(podTemplate).endTemplate()
                .endEntityOperator()
                .endSpec();

        if (!shouldKRaftBeUsed()) {
            builder
                    .editSpec()
                    .editZookeeper()
                    .withNewTemplate().withPod(podTemplate).endTemplate()
                    .endZookeeper()
                    .endSpec();
        }

        return self();
    }

    public static boolean shouldKRaftBeUsed() {
        if (USE_KRAFT == null) {
            ComparableVersion kafkaVersion = new ComparableVersion(ConfigProperties.STRIMZI_VERSION_KAFKA);
            ComparableVersion strimziVersion = new ComparableVersion(ConfigProperties.STRIMZI_OPERATOR_VERSION);

            USE_KRAFT = false;
            if (ConfigProperties.FORCE_KRAFT) {
                USE_KRAFT = true;
                LOGGER.info("KRaft forced by configuration.");
            } else if (kafkaVersion.compareTo(new ComparableVersion("4.0.0")) >= 0) {
                USE_KRAFT = true;
                LOGGER.info("Kafka version >= 4.0.0 detected.");
            } else if (!ConfigProperties.PRODUCT_BUILD && strimziVersion.compareTo(new ComparableVersion("0.46.0")) >= 0) {
                USE_KRAFT = true;
                LOGGER.info("Strimzi version >= 0.46.0.");
            }

            if (USE_KRAFT) {
                LOGGER.info("Using Kafka with KRaft is enabled.");
            } else {
                LOGGER.warn("Using Kafka with Zookeeper is enabled. This way will become deprecated soon.");
            }
        }
        return USE_KRAFT;
    }

    private static KafkaClusterSpec defaultKafkaSpec() {
        Map<String, Object> config = defaultKafkaConfig();
        List<GenericKafkaListener> listeners = defaultKafkaListeners();

        KafkaClusterSpecBuilder kafkaClusterSpec = new KafkaClusterSpecBuilder()
                .withConfig(config)
                .withVersion(STRIMZI_VERSION_KAFKA)
                .withListeners(listeners);

        if (!shouldKRaftBeUsed()) {
            kafkaClusterSpec
                    .withStorage(new EphemeralStorage())
                    .withReplicas(3);
        }
        return kafkaClusterSpec.build();
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

        if (shouldKRaftBeUsed()) {
            config.put("default.replication.factor", 3);
            config.put("min.insync.replicas", 2);
        }

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

    public KafkaNodePool defaultKafkaNodePool() {
        LOGGER.info("Creating Node Pool for kafka cluster: " + DEFAULT_KAFKA_NAME);
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                .withName(DEFAULT_NODE_POOL_NAME)
                .withLabels(Map.of("strimzi.io/cluster", DEFAULT_KAFKA_NAME))
                .endMetadata()
                .withNewSpec()
                .withReplicas(3)
                .withRoles(List.of(ProcessRoles.CONTROLLER, ProcessRoles.BROKER))
                .withStorage(new EphemeralStorageBuilder().build())
                .endSpec()
                .build();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerStatus;

/**
 * This class provides control over Kafka instance deployed in OpenShift
 *
 * @author Jakub Cechacek
 */
public class OcpKafkaController implements KafkaController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaController.class);

    private final OpenShiftClient ocp;
    private final String project;
    private final String name;
    private final StrimziOperatorController operatorController;

    private Kafka kafka;

    public OcpKafkaController(Kafka kafka, StrimziOperatorController operatorController, OpenShiftClient ocp) {
        this.kafka = kafka;
        this.name = kafka.getMetadata().getName();
        this.ocp = ocp;
        this.project = kafka.getMetadata().getNamespace();
        this.operatorController = operatorController;
    }

    @Override
    public String getPublicBootstrapAddress() {
        if (isRunningFromOcp()) {
            return getBootstrapAddress();
        }
        List<ListenerStatus> listeners = kafka.getStatus().getListeners();
        ListenerStatus listener = listeners.stream()
                .filter(l -> l.getType().equalsIgnoreCase("external"))
                .findAny().orElseThrow(() -> new IllegalStateException("No external listener found for Kafka cluster " + kafka.getMetadata().getName()));
        ListenerAddress address = listener.getAddresses().get(0);
        return address.getHost() + ":" + address.getPort();
    }

    @Override
    public String getBootstrapAddress() {
        return name + "-kafka-bootstrap." + project + ".svc.cluster.local:9092";
    }

    /**
     * Returns bootstrap to be used by KC.
     * The address is local.
     *
     * @return bootstrap
     */
    public String getLocalBootstrapAddress() {
        return name + "-kafka-bootstrap:9093";
    }

    /**
     * Deploy kafka topic from given CR
     *
     * @param yamlPath path to yaml descript
     * @return created topic
     * @throws InterruptedException
     */
    public KafkaTopic deployTopic(String yamlPath) throws InterruptedException {
        LOGGER.info("Deploying Kafka topic from " + yamlPath);
        KafkaTopic topic = topicOperation().createOrReplace(YAML.fromResource(yamlPath, KafkaTopic.class));
        return waitForKafkaTopic(topic.getMetadata().getName());
    }

    @Override
    public boolean undeploy() {
        return Crds.kafkaOperation(ocp).delete(kafka);
    }

    @Override
    public void waitForCluster() throws InterruptedException {
        LOGGER.info("Waiting for Kafka cluster '" + name + "'");
        kafka = kafkaOperation()
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, scaled(5), MINUTES);
    }

    /**
     * Waits until topic is properly deployed.
     *
     * @param name name of the topic
     * @throws InterruptedException     on wait error
     * @throws IllegalArgumentException when deployment doesn't use custom resources
     */
    private KafkaTopic waitForKafkaTopic(String name) throws InterruptedException {
        return topicOperation()
                .withName(name)
                .waitUntilCondition(WaitConditions::kafkaReadyCondition, scaled(5), MINUTES);
    }

    private NonNamespaceOperation<KafkaTopic, KafkaTopicList, Resource<KafkaTopic>> topicOperation() {
        return Crds.topicOperation(ocp).inNamespace(project);
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }

}

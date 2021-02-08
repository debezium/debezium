/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.debezium.testing.openshift.tools.YAML;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.status.ListenerAddress;
import io.strimzi.api.kafka.model.status.ListenerStatus;

import okhttp3.OkHttpClient;

/**
 * This class provides control over Kafka instance deployed in OpenShift
 * @author Jakub Cechacek
 */
public class KafkaController {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    private final Kafka kafka;
    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final String project;
    private final OpenShiftUtils ocpUtils;

    public KafkaController(Kafka kafka, OpenShiftClient ocp, OkHttpClient http) {
        this.kafka = kafka;
        this.ocp = ocp;
        this.http = http;
        this.project = kafka.getMetadata().getNamespace();
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    /**
     * @return host and port for public bootstrap service
     */
    public String getKafkaBootstrapAddress() {
        List<ListenerStatus> listeners = kafka.getStatus().getListeners();
        ListenerStatus listener = listeners.stream()
                .filter(l -> l.getType().equalsIgnoreCase("external"))
                .findAny().orElseThrow(() -> new IllegalStateException("No external listener found for Kafka cluster " + kafka.getMetadata().getName()));
        ListenerAddress address = listener.getAddresses().get(0);
        return address.getHost() + ":" + address.getPort();
    }

    public KafkaTopic deployTopic(String yamlPath) throws InterruptedException {
        LOGGER.info("Deploying Kafka topic from " + yamlPath);
        KafkaTopic topic = topicOperation().createOrReplace(YAML.fromResource(yamlPath, KafkaTopic.class));
        return waitForKafkaTopic(topic.getMetadata().getName());
    }

    /**
     * Undeploy this Kafka cluster by deleted related KafkaConnect CR
     * @return true if the CR was found and deleted
     */
    public boolean undeployCluster() {
        return Crds.kafkaOperation(ocp).delete(kafka);
    }

    /**
     * Waits until topic is properly deployed.
     * @param name name of the topic
     * @throws InterruptedException on wait error
     * @throws IllegalArgumentException when deployment doesn't use custom resources
     */
    public KafkaTopic waitForKafkaTopic(String name) throws InterruptedException {
        return topicOperation().withName(name).waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
    }

    private NonNamespaceOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> topicOperation() {
        return Crds.topicOperation(ocp).inNamespace(project);
    }
}

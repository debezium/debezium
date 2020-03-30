/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import static java.util.concurrent.TimeUnit.MINUTES;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.debezium.testing.openshift.tools.YAML;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public class KafkaDeployer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDeployer.class);

    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final OpenShiftUtils ocpUtils;
    private final String project;

    public KafkaDeployer(String project, OpenShiftClient ocp, OkHttpClient http) {
        this.project = project;
        this.ocp = ocp;
        this.http = http;
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    public KafkaDeployer(String project, OpenShiftClient ocp) {
        this(project, ocp, new OkHttpClient());
    }

    /**
     * Accessor for operator controller.
     * @return {@link OperatorController} instance for cluster operator in {@link #project}
     */
    public OperatorController getOperator() {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName("strimzi-cluster-operator").get();
        return new OperatorController(operator, ocp);
    }

    /**
     * Deploys Kafka Cluster
     * @param yamlPath path to CR descriptor (must be available on class path)
     * @return {@link KafkaController} instance for deployed cluster
     * @throws InterruptedException
     */
    public KafkaController deployKafkaCluster(String yamlPath) throws InterruptedException {
        LOGGER.info("Deploying Kafka from " + yamlPath);
        Kafka kafka = kafkaOperation().createOrReplace(YAML.fromResource(yamlPath, Kafka.class));

        kafka = waitForKafkaCluster(kafka.getMetadata().getName());
        return new KafkaController(kafka, ocp, http);
    }

    /**
     * Deploys Kafka Connect Cluster
     * @param yamlPath path to CR descriptor (must be available on class path)
     * @param useConnectorResources true if connector deployment should be managed by operator
     * @return {@link KafkaController} instance for deployed cluster
     */
    public KafkaConnectController deployKafkaConnectCluster(String yamlPath, String loggingYamlPath, boolean useConnectorResources) throws InterruptedException {
        LOGGER.info("Deploying KafkaConnect from " + yamlPath);

        ocp.configMaps().inNamespace(project).createOrReplace(YAML.fromResource(loggingYamlPath, ConfigMap.class));

        KafkaConnect kafkaConnect = YAML.fromResource(yamlPath, KafkaConnect.class);
        if (useConnectorResources) {
            kafkaConnect = new KafkaConnectBuilder(kafkaConnect)
                    .editMetadata()
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                    .endMetadata()
                    .build();
        }
        kafkaConnectOperation().createOrReplace(kafkaConnect);

        kafkaConnect = waitForConnectCluster(kafkaConnect.getMetadata().getName());
        return new KafkaConnectController(kafkaConnect, ocp, http, useConnectorResources);
    }

    public Kafka waitForKafkaCluster(String name) throws InterruptedException {
        return kafkaOperation().withName(name).waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
    }

    public KafkaConnect waitForConnectCluster(String name) throws InterruptedException {
        return kafkaConnectOperation().withName(name).waitUntilCondition(WaitConditions::kafkaReadyCondition, 5, MINUTES);
    }

    /**
     * Deploys pull secret and links it to "default" service account in the project
     * @param yamlPath path to Secret descriptor
     * @return deployed pull secret
     */
    public Secret deployPullSecret(String yamlPath) {
        LOGGER.info("Deploying Secret from " + yamlPath);
        return ocp.secrets().inNamespace(project).createOrReplace(YAML.from(yamlPath, Secret.class));
    }

    private NonNamespaceOperation<Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }

    private NonNamespaceOperation<KafkaConnect, KafkaConnectList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnectOperation() {
        return Crds.kafkaConnectOperation(ocp).inNamespace(project);
    }

}

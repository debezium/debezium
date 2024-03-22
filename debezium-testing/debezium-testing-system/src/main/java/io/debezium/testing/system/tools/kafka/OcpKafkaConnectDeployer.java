/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.kafka.builders.FabricKafkaConnectBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectList;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public class OcpKafkaConnectDeployer extends AbstractOcpDeployer<OcpKafkaConnectController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaConnectDeployer.class);

    private final FabricKafkaConnectBuilder fabricBuilder;
    private final ConfigMap configMap;
    private final StrimziOperatorController operatorController;

    public OcpKafkaConnectDeployer(
                                   String project,
                                   FabricKafkaConnectBuilder fabricBuilder,
                                   ConfigMap configMap,
                                   StrimziOperatorController operatorController,
                                   OpenShiftClient ocp,
                                   OkHttpClient http) {
        super(project, ocp, http);
        this.fabricBuilder = fabricBuilder;
        this.configMap = configMap;
        this.operatorController = operatorController;
    }

    /**
     * Deploys Kafka Connect Cluster
     * @return {@link OcpKafkaController} instance for deployed cluster
     */
    @Override
    public OcpKafkaConnectController deploy() throws InterruptedException {
        LOGGER.info("Deploying KafkaConnect");

        if (configMap != null) {
            deployConfigMap();
        }

        if (fabricBuilder.hasBuild()) {
            deployImageStream();
        }

        KafkaConnect kafkaConnect = fabricBuilder.build();
        kafkaConnect = kafkaConnectOperation().createOrReplace(kafkaConnect);

        OcpKafkaConnectController controller = new OcpKafkaConnectController(
                kafkaConnect,
                operatorController,
                ocp,
                http);
        controller.waitForCluster();

        return controller;

    }

    private void deployConfigMap() {
        ocp.configMaps().inNamespace(project).createOrReplace(configMap);
    }

    private void deployImageStream() {
        Optional<String> imageStream = fabricBuilder.imageStream();
        if (!imageStream.isPresent()) {
            throw new IllegalStateException("Image stream missing");
        }

        String[] image = fabricBuilder.imageStream().get().split(":", 2);

        ImageStream is = new ImageStreamBuilder()
                .withNewMetadata().withName(image[0]).endMetadata()
                .withNewSpec()
                .withNewLookupPolicy(true)
                .endSpec()
                .build();

        ocp.imageStreams()
                .inNamespace(project)
                .createOrReplace(is);
    }

    private NonNamespaceOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation() {
        return Crds.kafkaConnectOperation(ocp).inNamespace(project);
    }
}

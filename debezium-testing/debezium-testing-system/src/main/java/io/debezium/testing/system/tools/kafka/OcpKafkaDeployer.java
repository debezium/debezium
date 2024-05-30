/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.kafka.builders.FabricKafkaBuilder;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaList;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public final class OcpKafkaDeployer extends AbstractOcpDeployer<OcpKafkaController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaDeployer.class);

    private final FabricKafkaBuilder strimziBuilder;
    private final StrimziOperatorController operatorController;

    public OcpKafkaDeployer(String project, FabricKafkaBuilder strimziBuilder, StrimziOperatorController operatorController,
                            OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
        this.strimziBuilder = strimziBuilder;
        this.operatorController = operatorController;
    }

    /**
     * Deploys Kafka Cluster
     * @return {@link OcpKafkaController} instance for deployed cluster
     * @throws InterruptedException
     */
    @Override
    public OcpKafkaController deploy() throws InterruptedException {
        LOGGER.info("Deploying Kafka Cluster");
        Kafka kafka = kafkaOperation().createOrReplace(strimziBuilder.build());

        OcpKafkaController controller = new OcpKafkaController(kafka, operatorController, ocp);
        controller.waitForCluster();

        return controller;
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }
}

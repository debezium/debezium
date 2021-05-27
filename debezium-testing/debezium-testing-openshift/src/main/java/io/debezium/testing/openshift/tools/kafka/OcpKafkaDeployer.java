/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.AbstractOcpDeployer;
import io.debezium.testing.openshift.tools.Deployer;
import io.debezium.testing.openshift.tools.YAML;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public final class OcpKafkaDeployer extends AbstractOcpDeployer<OcpKafkaController> {

    /**
     * Builder for {@link OcpKafkaDeployer}
     */
    public static class Builder implements Deployer.Builder<OcpKafkaDeployer> {

        private String project;
        private String yamlPath;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;

        public Builder withProject(String project) {
            this.project = project;
            return this;
        }

        public Builder withOcpClient(OpenShiftClient ocpClient) {
            this.ocpClient = ocpClient;
            return this;
        }

        public Builder withHttpClient(OkHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder withYamlPath(String yamlPath) {
            this.yamlPath = yamlPath;
            return this;
        }

        @Override
        public OcpKafkaDeployer build() {
            return new OcpKafkaDeployer(project, yamlPath, ocpClient, httpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaDeployer.class);

    private final String yamlPath;

    private OcpKafkaDeployer(String project, String yamlPath, OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
        this.yamlPath = yamlPath;
    }

    /**
     * Deploys Kafka Cluster
     * @return {@link OcpKafkaController} instance for deployed cluster
     * @throws InterruptedException
     */
    @Override
    public OcpKafkaController deploy() throws InterruptedException {
        LOGGER.info("Deploying Kafka from " + yamlPath);
        Kafka kafka = kafkaOperation().createOrReplace(YAML.fromResource(yamlPath, Kafka.class));

        OcpKafkaController controller = new OcpKafkaController(kafka, ocp);
        controller.waitForCluster();

        return controller;
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }
}

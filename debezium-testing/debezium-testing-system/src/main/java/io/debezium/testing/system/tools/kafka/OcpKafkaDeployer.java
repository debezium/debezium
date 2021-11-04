/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.api.kafka.model.template.PodTemplateBuilder;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public final class OcpKafkaDeployer extends AbstractOcpDeployer<OcpKafkaController> {

    /**
     * Builder for {@link OcpKafkaDeployer}
     */
    public static class Builder implements Deployer.Builder<Builder, OcpKafkaDeployer> {

        private String project;
        private String yamlPath;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private StrimziOperatorController operatorController;

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

        public Builder withOperatorController(StrimziOperatorController operatorController) {
            this.operatorController = operatorController;
            return this;
        }

        @Override
        public OcpKafkaDeployer build() {
            return new OcpKafkaDeployer(project, yamlPath, operatorController, ocpClient, httpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaDeployer.class);

    private final String yamlPath;
    private final String pullSecretName;
    private final StrimziOperatorController operatorController;

    private OcpKafkaDeployer(String project, String yamlPath, StrimziOperatorController operatorController,
                             OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
        this.yamlPath = yamlPath;
        this.operatorController = operatorController;
        this.pullSecretName = this.operatorController.getPullSecretName();
    }

    /**
     * Deploys Kafka Cluster
     * @return {@link OcpKafkaController} instance for deployed cluster
     * @throws InterruptedException
     */
    @Override
    public OcpKafkaController deploy() throws InterruptedException {
        LOGGER.info("Deploying Kafka from " + yamlPath);
        Kafka kafka = YAML.fromResource(yamlPath, Kafka.class);
        KafkaBuilder builder = new KafkaBuilder(kafka);

        if (pullSecretName != null) {
            configurePullSecret(builder);
        }

        kafka = kafkaOperation().createOrReplace(builder.build());

        OcpKafkaController controller = new OcpKafkaController(kafka, operatorController, ocp);
        controller.waitForCluster();

        return controller;
    }

    public void configurePullSecret(KafkaBuilder builder) {
        PodTemplate podTemplate = new PodTemplateBuilder().addNewImagePullSecret(pullSecretName).build();

        builder
                .editSpec()
                .editKafka()
                .withNewTemplate().withPod(podTemplate).endTemplate()
                .endKafka()
                .endSpec();
    }

    private NonNamespaceOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOperation() {
        return Crds.kafkaOperation(ocp).inNamespace(project);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import static io.debezium.testing.system.tools.ConfigProperties.STRIMZI_KC_IMAGE;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.kafka.builders.kafka.StrimziKafkaConnectBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.KafkaConnect;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public class OcpKafkaConnectDeployer extends AbstractOcpDeployer<OcpKafkaConnectController> {

    /**
     * Builder for {@link OcpKafkaConnectDeployer}
     */
    public static class Builder implements Deployer.Builder<Builder, OcpKafkaConnectDeployer> {

        private String project;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private ConfigMap configMap;
        private StrimziOperatorController operatorController;
        private final StrimziKafkaConnectBuilder strimziBuilder;

        public Builder(StrimziKafkaConnectBuilder strimziBuilder) {
            this.strimziBuilder = strimziBuilder;
        }

        public OcpKafkaConnectDeployer.Builder withProject(String project) {
            this.project = project;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withOcpClient(OpenShiftClient ocpClient) {
            this.ocpClient = ocpClient;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withHttpClient(OkHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withLoggingAndMetricsFromCfgMap(String cfgYamlPath) {
            this.configMap = YAML.fromResource(cfgYamlPath, ConfigMap.class);
            strimziBuilder
                    .withLoggingFromConfigMap(configMap)
                    .withMetricsFromConfigMap(configMap);
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withConnectorResources(boolean connectorResources) {
            if (connectorResources) {
                strimziBuilder.withConnectorResources();
            }
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withOperatorController(StrimziOperatorController operatorController) {
            this.operatorController = operatorController;
            operatorController.getPullSecretName().ifPresent(strimziBuilder::withPullSecret);

            return this;
        }

        @Override
        public OcpKafkaConnectDeployer build() {
            return new OcpKafkaConnectDeployer(
                    project,
                    strimziBuilder,
                    configMap,
                    operatorController,
                    ocpClient,
                    httpClient);
        }

        public Builder withKcBuild(boolean kcBuild) {
            if (kcBuild) {
                strimziBuilder
                        .withBuild()
                        .withStandardPlugins();
            }
            else {
                strimziBuilder.withImage(STRIMZI_KC_IMAGE);
            }
            return this;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaConnectDeployer.class);

    private final StrimziKafkaConnectBuilder strimziBuilder;
    private final ConfigMap configMap;
    private final StrimziOperatorController operatorController;

    private OcpKafkaConnectDeployer(
                                    String project,
                                    StrimziKafkaConnectBuilder strimziBuilder,
                                    ConfigMap configMap,
                                    StrimziOperatorController operatorController,
                                    OpenShiftClient ocp,
                                    OkHttpClient http) {
        super(project, ocp, http);
        this.strimziBuilder = strimziBuilder;
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

        if (strimziBuilder.hasBuild()) {
            deployImageStream();
        }

        KafkaConnect kafkaConnect = strimziBuilder.build();
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
        Optional<String> imageStream = strimziBuilder.imageStream();
        if (!imageStream.isPresent()) {
            throw new IllegalStateException("Image stream missing");
        }

        String[] image = strimziBuilder.imageStream().get().split(":", 2);

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

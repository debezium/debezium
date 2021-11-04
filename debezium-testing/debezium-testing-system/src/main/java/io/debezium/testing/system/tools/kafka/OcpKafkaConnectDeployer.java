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
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplateBuilder;

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
        private String yamlPath;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private String cfgYamlPath;
        private boolean connectorResources;
        private boolean exposedMetrics;
        private boolean exposedApi;
        private StrimziOperatorController operatorController;

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

        public OcpKafkaConnectDeployer.Builder withYamlPath(String yamlPath) {
            this.yamlPath = yamlPath;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withCfgYamlPath(String cfgYamlPath) {
            this.cfgYamlPath = cfgYamlPath;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withConnectorResources(boolean value) {
            this.connectorResources = value;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withExposedApi(boolean value) {
            this.exposedApi = value;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withExposedMetrics(boolean value) {
            this.exposedMetrics = value;
            return this;
        }

        public OcpKafkaConnectDeployer.Builder withOperatorController(StrimziOperatorController operatorController) {
            this.operatorController = operatorController;
            return this;
        }

        @Override
        public OcpKafkaConnectDeployer build() {
            return new OcpKafkaConnectDeployer(
                    project,
                    yamlPath,
                    cfgYamlPath,
                    connectorResources,
                    operatorController,
                    exposedApi,
                    exposedMetrics,
                    ocpClient, httpClient);
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaConnectDeployer.class);

    private final String yamlPath;
    private final String cfgYamlPath;
    private final boolean connectorResources;
    private final StrimziOperatorController operatorController;
    private final String pullSecretName;
    private final boolean exposedApi;
    private final boolean exposedMetrics;

    private OcpKafkaConnectDeployer(
                                    String project,
                                    String yamlPath,
                                    String cfgYamlPath,
                                    boolean connectorResources,
                                    StrimziOperatorController operatorController,
                                    boolean exposedApi,
                                    boolean exposedMetrics,
                                    OpenShiftClient ocp,
                                    OkHttpClient http) {
        super(project, ocp, http);
        this.yamlPath = yamlPath;
        this.cfgYamlPath = cfgYamlPath;
        this.connectorResources = connectorResources;
        this.operatorController = operatorController;
        this.pullSecretName = operatorController.getPullSecretName();
        this.exposedApi = exposedApi;
        this.exposedMetrics = exposedMetrics;
    }

    /**
     * Deploys Kafka Connect Cluster
     * @return {@link OcpKafkaController} instance for deployed cluster
     */
    @Override
    public OcpKafkaConnectController deploy() throws InterruptedException {
        LOGGER.info("Deploying KafkaConnect from " + yamlPath);

        KafkaConnect kafkaConnect = YAML.fromResource(yamlPath, KafkaConnect.class);
        Build kcBuild = kafkaConnect.getSpec().getBuild();
        KafkaConnectBuilder kcBuilder = new KafkaConnectBuilder(kafkaConnect);

        if (cfgYamlPath != null) {
            deployConfigMap();
        }

        if (connectorResources) {
            configureConnectorResources(kcBuilder);
        }

        if (pullSecretName != null) {
            configurePullSecret(kcBuilder, kcBuild);
        }

        if (kcBuild != null && "imagestream".equals(kcBuild.getOutput().getType())) {
            deployImageStream(kcBuild);
        }

        kafkaConnect = kcBuilder.build();
        kafkaConnect = kafkaConnectOperation().createOrReplace(kafkaConnect);

        OcpKafkaConnectController controller = new OcpKafkaConnectController(
                kafkaConnect,
                operatorController,
                ocp,
                http,
                connectorResources);
        controller.waitForCluster();

        return controller;

    }

    private void deployConfigMap() {
        ocp.configMaps().inNamespace(project)
                .createOrReplace(YAML.fromResource(cfgYamlPath, ConfigMap.class));
    }

    private void deployImageStream(Build kcBuild) {
        String[] image = kcBuild.getOutput().getImage().split(":", 2);
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

    private void configurePullSecret(KafkaConnectBuilder kcBuilder, Build kcBuild) {
        KafkaConnectTemplate template = kcBuilder.buildSpec().getTemplate();
        KafkaConnectTemplateBuilder templateBuilder = new KafkaConnectTemplateBuilder(template);

        if (kcBuild == null) {
            templateBuilder.withNewPod().addNewImagePullSecret(pullSecretName);
        }
        else {
            templateBuilder.withNewBuildConfig().withPullSecret(pullSecretName).endBuildConfig();
        }
        kcBuilder
                .editSpec()
                .withTemplate(templateBuilder.build())
                .endSpec();
    }

    private void configureConnectorResources(KafkaConnectBuilder kcBuilder) {
        kcBuilder
                .editMetadata()
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata();
    }

    private NonNamespaceOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation() {
        return Crds.kafkaConnectOperation(ocp).inNamespace(project);
    }
}

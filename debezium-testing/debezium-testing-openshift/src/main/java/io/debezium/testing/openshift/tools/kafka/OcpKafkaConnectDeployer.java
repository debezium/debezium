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
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Kafka & Kafka Connect clusters via Strimzi
 * @author Jakub Cechacek
 */
public class OcpKafkaConnectDeployer extends AbstractOcpDeployer<OcpKafkaConnectController> {

    /**
     * Builder for {@link OcpKafkaConnectDeployer}
     */
    public static class Builder implements Deployer.Builder<OcpKafkaConnectDeployer> {

        private String project;
        private String yamlPath;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private String cfgYamlPath;
        private boolean connectorResources;
        private boolean exposedMetrics;
        private boolean exposedApi;

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

        @Override
        public OcpKafkaConnectDeployer build() {
            return new OcpKafkaConnectDeployer(
                    project,
                    yamlPath,
                    cfgYamlPath,
                    connectorResources,
                    exposedApi,
                    exposedMetrics,
                    ocpClient, httpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaConnectDeployer.class);

    private final String yamlPath;
    private final String cfgYamlPath;
    private final boolean connectorResources;
    private final boolean exposedApi;
    private final boolean exposedMetrics;

    private OcpKafkaConnectDeployer(
                                    String project,
                                    String yamlPath,
                                    String cfgYamlPath,
                                    boolean connectorResources,
                                    boolean exposedApi,
                                    boolean exposedMetrics,
                                    OpenShiftClient ocp,
                                    OkHttpClient http) {
        super(project, ocp, http);
        this.yamlPath = yamlPath;
        this.cfgYamlPath = cfgYamlPath;
        this.connectorResources = connectorResources;
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

        ocp.configMaps().inNamespace(project)
                .createOrReplace(YAML.fromResource(cfgYamlPath, ConfigMap.class));

        KafkaConnect kafkaConnect = YAML.fromResource(yamlPath, KafkaConnect.class);
        if (connectorResources) {
            kafkaConnect = enableConnectorResources(kafkaConnect);
        }
        kafkaConnect = kafkaConnectOperation().createOrReplace(kafkaConnect);

        OcpKafkaConnectController controller = new OcpKafkaConnectController(
                kafkaConnect,
                ocp,
                http,
                connectorResources);
        controller.waitForCluster();

        return controller;

    }

    private KafkaConnect enableConnectorResources(KafkaConnect baseReource) {
        return new KafkaConnectBuilder(baseReource)
                .editMetadata()
                .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .build();
    }

    private NonNamespaceOperation<KafkaConnect, KafkaConnectList, Resource<KafkaConnect>> kafkaConnectOperation() {
        return Crds.kafkaConnectOperation(ocp).inNamespace(project);
    }
}

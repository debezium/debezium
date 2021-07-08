/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.OperatorController;
import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaController;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Apicurio service registry OCP deployment
 * @author Jakub Cechacek
 */
public class OcpRegistryDeployer extends AbstractOcpDeployer<OcpRegistryController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpRegistryDeployer.class);

    private final KafkaController kafkaController;
    private final String yamlPath;
    private final String idTopicYamlPath;
    private final String storageTopicYamlPath;

    private OcpRegistryDeployer(
                                String project,
                                String yamlPath,
                                String storageTopicYamlPath,
                                String idTopicYamlPath,
                                KafkaController kafkaController,
                                OpenShiftClient ocp,
                                OkHttpClient http) {
        super(project, ocp, http);
        this.kafkaController = kafkaController;
        this.yamlPath = yamlPath;
        this.storageTopicYamlPath = storageTopicYamlPath;
        this.idTopicYamlPath = idTopicYamlPath;
    }

    /**
     * Accessor for operator controller.
     * @return {@link OperatorController} instance for cluster operator in {@link #project}
     */
    public OperatorController getOperator() {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName("apicurio-registry-operator").get();
        return new OperatorController(operator, Collections.singletonMap("name", "apicurio-registry-operator"), ocp);
    }

    public NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.apiextensions().v1().customResourceDefinitions()
                .load(OcpRegistryDeployer.class.getResourceAsStream("/crds/apicur.io_apicurioregistries_crd.yaml"))
                .get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(crd);
        return ocp.customResources(context, ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    public ApicurioRegistry waitForRegistry(String name) throws InterruptedException {
        LOGGER.info("Waiting for deployments of registry '" + name + "'");
        await()
                .atMost(scaled(1), MINUTES)
                .pollInterval(5, SECONDS)
                .until(() -> !getRegistryDeployments(name).isEmpty());

        DeploymentConfig dc = getRegistryDeployments(name).get(0);
        ocp.deploymentConfigs()
                .inNamespace(project)
                .withName(dc.getMetadata().getName())
                .waitUntilCondition(WaitConditions::deploymentAvailableCondition, scaled(5), MINUTES);

        return registryOperation().withName(name).get();
    }

    private List<DeploymentConfig> getRegistryDeployments(String name) {
        return ocp.deploymentConfigs().inNamespace(project).withLabel("app", name).list().getItems();
    }

    @Override
    public OcpRegistryController deploy() throws InterruptedException {
        LOGGER.info("Deploying Apicurio Registry from " + yamlPath);
        // TODO: unsafe cast (acceptable for now since we have only a single implementation of kafka controller)
        ((OcpKafkaController) kafkaController).deployTopic(storageTopicYamlPath);
        ((OcpKafkaController) kafkaController).deployTopic(idTopicYamlPath);

        ApicurioRegistry registry = YAML.fromResource(yamlPath, ApicurioRegistry.class);
        registry = registryOperation().createOrReplace(registry);
        registry = waitForRegistry(registry.getMetadata().getName());

        return new OcpRegistryController(registry, ocp, http);
    }

    public static class Builder implements Deployer.Builder<OcpRegistryDeployer> {

        private String project;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private String yamlPath;
        private String storageTopicYamlPath;
        private String idTopicYamlPath;
        private KafkaController kafkaController;

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

        public Builder withTopicsYamlPath(String storageTopicYamlPath, String idTopicYamlPath) {
            this.storageTopicYamlPath = storageTopicYamlPath;
            this.idTopicYamlPath = idTopicYamlPath;
            return this;
        }

        public Builder withKafkaController(KafkaController kafkaController) {
            this.kafkaController = kafkaController;
            return this;

        }

        @Override
        public OcpRegistryDeployer build() {
            return new OcpRegistryDeployer(
                    project,
                    yamlPath,
                    storageTopicYamlPath,
                    idTopicYamlPath,
                    kafkaController,
                    ocpClient,
                    httpClient);
        }
    }
}

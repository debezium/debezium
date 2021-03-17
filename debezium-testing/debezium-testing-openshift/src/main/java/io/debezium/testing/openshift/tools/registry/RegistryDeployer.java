/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.registry;

import static io.debezium.testing.openshift.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.apicurio.registry.operator.api.model.DoneableApicurioRegistry;
import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.OperatorController;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.debezium.testing.openshift.tools.YAML;
import io.debezium.testing.openshift.tools.kafka.KafkaController;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.internal.KubernetesDeserializer;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 *
 * @author Jakub Cechacek
 */
public class RegistryDeployer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryDeployer.class);

    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final OpenShiftUtils ocpUtils;
    private final String project;
    private final KafkaController kafkaController;

    public RegistryDeployer(String project, OpenShiftClient ocp, OkHttpClient http, KafkaController kafkaController) {
        this.project = project;
        this.ocp = ocp;
        this.http = http;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.kafkaController = kafkaController;
    }

    /**
     * Accessor for operator controller.
     * @return {@link OperatorController} instance for cluster operator in {@link #project}
     */
    public OperatorController getOperator() {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName("apicurio-registry-operator").get();
        return new OperatorController(operator, Collections.singletonMap("name", "apicurio-registry-operator"), ocp);
    }

    /**
     * Deploys Kafka Cluster
     * @param yamlPath path to CR descriptor (must be available on class path)
     * @return {@link RegistryController} instance for deployed registry
     * @throws InterruptedException
     */
    public RegistryController deployRegistry(String yamlPath, String storageTopicYamlPath, String idTopicYamlPath) throws InterruptedException {
        LOGGER.info("Deploying Apicurio Registry from " + yamlPath);
        kafkaController.deployTopic(storageTopicYamlPath);
        kafkaController.deployTopic(idTopicYamlPath);

        ApicurioRegistry registry = YAML.fromResource(yamlPath, ApicurioRegistry.class);
        registry = registryOperation().createOrReplace(registry);
        registry = waitForRegistry(registry.getMetadata().getName());

        return new RegistryController(registry, ocp, http);
    }

    public NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, DoneableApicurioRegistry, Resource<ApicurioRegistry, DoneableApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.customResourceDefinitions().load(RegistryDeployer.class.getResourceAsStream("/crds/apicur.io_apicurioregistries_crd.yaml"))
                .get();
        KubernetesDeserializer.registerCustomKind("apicur.io/v1alpha1", "ApicurioRegistry", ApicurioRegistry.class);
        return ocp.customResources(crd, ApicurioRegistry.class, ApicurioRegistryList.class, DoneableApicurioRegistry.class).inNamespace(project);
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
        return ocp.deploymentConfigs().inNamespace(project).withLabel("apicur.io/name", name).list().getItems();
    }
}

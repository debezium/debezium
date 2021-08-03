/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.kafka.KafkaController;
import io.debezium.testing.system.tools.kafka.OcpKafkaController;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Apicurio service registry OCP deployment
 * @author Jakub Cechacek
 */
public class OcpApicurioV1Deployer extends AbstractOcpApicurioDeployer<OcpApicurioV1Controller> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioV1Deployer.class);
    public static final String APICURIO_CRD_DESCRIPTOR = "/crd/v1beta1/apicurioregistries_crd.yaml";

    private final KafkaController kafkaController;
    private final String idTopicYamlPath;
    private final String storageTopicYamlPath;

    private OcpApicurioV1Deployer(
                                  String project,
                                  String yamlPath,
                                  String storageTopicYamlPath,
                                  String idTopicYamlPath,
                                  KafkaController kafkaController,
                                  OpenShiftClient ocp,
                                  OkHttpClient http) {
        super(project, yamlPath, ocp, http);
        this.kafkaController = kafkaController;
        this.storageTopicYamlPath = storageTopicYamlPath;
        this.idTopicYamlPath = idTopicYamlPath;
    }

    @Override
    public OcpApicurioV1Controller deploy() throws InterruptedException {
        LOGGER.info("Deploying kafka topics from  " + storageTopicYamlPath + ", " + idTopicYamlPath);
        // TODO: unsafe cast (acceptable for now since we only support OCP deployment for registry)
        ((OcpKafkaController) kafkaController).deployTopic(storageTopicYamlPath);
        ((OcpKafkaController) kafkaController).deployTopic(idTopicYamlPath);

        return super.deploy();
    }

    @Override
    protected OcpApicurioV1Controller getController(ApicurioRegistry registry) {
        return new OcpApicurioV1Controller(registry, ocp, http);
    }

    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.apiextensions().v1beta1().customResourceDefinitions()
                .load(OcpApicurioV1Deployer.class.getResourceAsStream(APICURIO_CRD_DESCRIPTOR))
                .get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(crd);
        return ocp.customResources(context, ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    public static class Builder extends AbstractOcpApicurioDeployer.RegistryBuilder<Builder, OcpApicurioV1Deployer> {

        private String storageTopicYamlPath;
        private String idTopicYamlPath;
        private KafkaController kafkaController;

        public Builder withTopicsYamlPath(String storageTopicYamlPath, String idTopicYamlPath) {
            this.storageTopicYamlPath = storageTopicYamlPath;
            this.idTopicYamlPath = idTopicYamlPath;
            return self();
        }

        public Builder withKafkaController(KafkaController kafkaController) {
            this.kafkaController = kafkaController;
            return self();

        }

        @Override
        public OcpApicurioV1Deployer build() {
            return new OcpApicurioV1Deployer(
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

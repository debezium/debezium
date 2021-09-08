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
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Apicurio service registry OCP deployment
 * @author Jakub Cechacek
 */
public class OcpApicurioV2Deployer extends AbstractOcpApicurioDeployer<OcpApicurioV2Controller> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioV2Deployer.class);
    public static final String APICURIO_CRD_DESCRIPTOR = "/crd/v1/apicurioregistries_crd.yaml";

    private OcpApicurioV2Deployer(
                                  String project,
                                  String yamlPath,
                                  OpenShiftClient ocp,
                                  OkHttpClient http) {
        super(project, yamlPath, ocp, http);
    }

    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.apiextensions().v1().customResourceDefinitions()
                .load(OcpApicurioV2Deployer.class.getResourceAsStream(APICURIO_CRD_DESCRIPTOR))
                .get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(crd);
        return ocp.customResources(context, ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    @Override
    protected OcpApicurioV2Controller getController(ApicurioRegistry registry) {
        return new OcpApicurioV2Controller(registry, ocp, http);
    }

    public static class Builder extends AbstractOcpApicurioDeployer.RegistryBuilder<Builder, OcpApicurioV2Deployer> {

        @Override
        public OcpApicurioV2Deployer build() {
            return new OcpApicurioV2Deployer(
                    project,
                    yamlPath,
                    ocpClient,
                    httpClient);
        }
    }
}

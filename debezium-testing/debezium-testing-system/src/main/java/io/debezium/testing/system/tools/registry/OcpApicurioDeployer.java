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
public class OcpApicurioDeployer extends AbstractOcpApicurioDeployer<OcpApicurioController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioDeployer.class);
    public static final String APICURIO_CRD_DESCRIPTOR = "/crd/v1/apicurioregistries_crd.yaml";

    private OcpApicurioDeployer(
                                String project,
                                String yamlPath,
                                OpenShiftClient ocp,
                                OkHttpClient http) {
        super(project, yamlPath, ocp, http);
    }

    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.apiextensions().v1().customResourceDefinitions()
                .load(OcpApicurioDeployer.class.getResourceAsStream(APICURIO_CRD_DESCRIPTOR))
                .get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(crd);
        return ocp.customResources(context, ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    @Override
    protected OcpApicurioController getController(ApicurioRegistry registry) {
        return new OcpApicurioController(registry, ocp, http);
    }

    public static class Builder extends AbstractOcpApicurioDeployer.RegistryBuilder<Builder, OcpApicurioDeployer> {

        @Override
        public OcpApicurioDeployer build() {
            return new OcpApicurioDeployer(
                    project,
                    yamlPath,
                    ocpClient,
                    httpClient);
        }
    }
}

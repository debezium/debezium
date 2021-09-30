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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * This class provides control over Apicurio registry instance deployed in OpenShift
 * @author Jakub Cechacek
 */
public class OcpApicurioV1Controller extends AbstractOcpApicurioController implements RegistryController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioV1Controller.class);

    public static final String APICURIO_CRD_DESCRIPTOR = "/crd/v1beta1/apicurioregistries_crd.yaml";

    public OcpApicurioV1Controller(ApicurioRegistry registry, OpenShiftClient ocp, OkHttpClient http) {
        super(registry, ocp, http);
    }

    @Override
    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        CustomResourceDefinition crd = ocp.apiextensions().v1beta1().customResourceDefinitions()
                .load(OcpApicurioV1Controller.class.getResourceAsStream(APICURIO_CRD_DESCRIPTOR))
                .get();
        CustomResourceDefinitionContext context = CustomResourceDefinitionContext.fromCrd(crd);
        return ocp.customResources(context, ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    @Override
    public String getRegistryApiAddress() {
        return getRegistryAddress() + "/api";
    }

    @Override
    public String getPublicRegistryApiAddress() {
        return getPublicRegistryAddress() + "/api";
    }

    @Override
    public void waitForRegistry() throws InterruptedException {
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

        registry = registryOperation().withName(name).get();
    }

    private List<DeploymentConfig> getRegistryDeployments(String name) {
        return ocp.deploymentConfigs().inNamespace(project).withLabel("app", name).list().getItems();
    }
}

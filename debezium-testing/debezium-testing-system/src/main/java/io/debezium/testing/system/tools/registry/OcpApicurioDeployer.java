/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.registry.builders.FabricApicurioBuilder;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Apicurio service registry OCP deployment
 *
 * @author Jakub Cechacek
 */

public class OcpApicurioDeployer extends AbstractOcpDeployer<OcpApicurioController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioDeployer.class);
    private final FabricApicurioBuilder fabricBuilder;

    public OcpApicurioDeployer(String project, FabricApicurioBuilder fabricBuilder, OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
        this.fabricBuilder = fabricBuilder;
    }

    @Override
    public OcpApicurioController deploy() throws InterruptedException {
        LOGGER.info("Deploying Apicurio Registry to '" + project + "'");

        ApicurioRegistry registry = fabricBuilder.build();
        registry = registryOperation().createOrReplace(registry);

        OcpApicurioController controller = getController(registry);
        controller.waitForRegistry();

        return controller;
    }

    protected OcpApicurioController getController(ApicurioRegistry registry) {
        return new OcpApicurioController(registry, ocp, http);
    }

    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        return ocp.resources(ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }
}

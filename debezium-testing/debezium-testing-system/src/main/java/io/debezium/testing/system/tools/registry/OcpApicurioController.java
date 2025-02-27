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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * This class provides control over Apicurio registry instance deployed in OpenShift
 *
 * @author Jakub Cechacek
 */
public class OcpApicurioController implements RegistryController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioController.class);

    public static final String APICURIO_NAME_LBL = "apicur.io/name";

    protected final OpenShiftClient ocp;
    protected final OkHttpClient http;
    protected final String project;
    protected final String name;
    protected final OpenShiftUtils ocpUtils;
    protected ApicurioRegistry registry;

    public OcpApicurioController(ApicurioRegistry registry, OpenShiftClient ocp, OkHttpClient http) {
        this.registry = registry;
        this.ocp = ocp;
        this.http = http;
        this.project = registry.getMetadata().getNamespace();
        this.name = registry.getMetadata().getName();
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    protected NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation() {
        return ocp.resources(ApicurioRegistry.class, ApicurioRegistryList.class).inNamespace(project);
    }

    protected String getPublicRegistryAddress() {
        await().atMost(scaled(30), TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() -> !getRoutes().isEmpty());

        return getRoutes().get(0).getSpec().getHost();
    }

    private List<Route> getRoutes() {
        return ocp.routes().inNamespace(project).withLabel(APICURIO_NAME_LBL, name).list().getItems();
    }

    protected Service getRegistryService() {
        List<Service> items = ocp.services().inNamespace(project).withLabel(APICURIO_NAME_LBL, name).list().getItems();
        if (items.isEmpty()) {
            throw new IllegalStateException("No service for registry '" + registry.getMetadata().getName() + "'");
        }

        return items.get(0);
    }

    protected String getRegistryAddress() {
        Service s = getRegistryService();
        return "http://" + s.getMetadata().getName() + "." + project + ".svc.cluster.local:8080";
    }

    @Override
    public String getRegistryApiAddress() {
        return getRegistryAddress() + "/apis/registry/v2";
    }

    @Override
    public String getPublicRegistryApiAddress() {
        return "http://" + getPublicRegistryAddress() + "/apis/registry/v2";
    }

    @Override
    public void waitForRegistry() throws InterruptedException {
        LOGGER.info("Waiting for deployments of registry '" + name + "' in '" + project + "'");
        await().atMost(scaled(1), MINUTES)
                .pollInterval(5, SECONDS)
                .until(() -> !getRegistryDeployments(name).isEmpty());

        Deployment deployment = getRegistryDeployments(name).get(0);
        ocp.apps()
                .deployments()
                .inNamespace(project)
                .withName(deployment.getMetadata().getName())
                .waitUntilCondition(WaitConditions::deploymentAvailableCondition, scaled(5), MINUTES);

        registry = registryOperation().withName(name).get();
    }

    private List<Deployment> getRegistryDeployments(String name) {
        return ocp.apps().deployments().inNamespace(project).withLabel("app", name).list().getItems();
    }

    @Override
    public boolean undeploy() {
        try {
            registryOperation().resource(registry).delete();
            registryOperation()
                    .resource(registry)
                    .waitUntilCondition(WaitConditions::apicurioResourceDeleted, scaled(1), MINUTES);
        }
        catch (Exception exception) {
            LOGGER.error("Apicurio registry was not deleted");
            return false;
        }
        return true;
    }
}

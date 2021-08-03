/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public abstract class AbstractOcpApicurioController implements RegistryController {

    public static final String APICURIO_NAME_LBL = "apicur.io/name";

    protected final OpenShiftClient ocp;
    protected final OkHttpClient http;
    protected final String project;
    protected final String name;
    protected final OpenShiftUtils ocpUtils;
    protected ApicurioRegistry registry;

    public AbstractOcpApicurioController(ApicurioRegistry registry, OpenShiftClient ocp, OkHttpClient http) {
        this.registry = registry;
        this.ocp = ocp;
        this.http = http;
        this.project = registry.getMetadata().getNamespace();
        this.name = registry.getMetadata().getName();
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    protected String getRegistryAddress() {
        Service s = getRegistryService();
        return "http://" + s.getMetadata().getName() + "." + project + ".svc.cluster.local:8080";
    }

    protected String getPublicRegistryAddress() {
        await()
                .atMost(scaled(30), TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() -> !getRoutes().isEmpty());

        return getRoutes().get(0).getSpec().getHost();
    }

    private List<Route> getRoutes() {
        return ocp.routes().inNamespace(project)
                .withLabel(APICURIO_NAME_LBL, name)
                .list().getItems();
    }

    protected Service getRegistryService() {
        List<Service> items = ocp.services().inNamespace(project).withLabel(APICURIO_NAME_LBL, name).list().getItems();
        if (items.isEmpty()) {
            throw new IllegalStateException("No service for registry '" + registry.getMetadata().getName() + "'");
        }

        return items.get(0);
    }

    @Override
    public boolean undeploy() {
        return registryOperation().delete(registry);
    }

    protected abstract NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation();

}

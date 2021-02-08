/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.registry;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.apicurio.registry.operator.api.model.DoneableApicurioRegistry;
import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * This class provides control over Kafka instance deployed in OpenShift
 * @author Jakub Cechacek
 */
public class RegistryController {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryController.class);

    private final ApicurioRegistry registry;
    private final OpenShiftClient ocp;
    private final OkHttpClient http;
    private final String project;
    private final OpenShiftUtils ocpUtils;

    public RegistryController(ApicurioRegistry registry, OpenShiftClient ocp, OkHttpClient http) {
        this.registry = registry;
        this.ocp = ocp;
        this.http = http;
        this.project = registry.getMetadata().getNamespace();
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    /**
     * @return registry url
     */
    public String getRegistryAddress() {
        Service s = getRegistryService();
        return "http://" + s.getMetadata().getName() + "." + ConfigProperties.OCP_PROJECT_REGISTRY + ".svc.cluster.local:8080";
    }

    /**
     * @return registry url
     */
    public String getRegistryApiAddress() {
        return getRegistryAddress() + "/api";
    }

    /**
     * @return registry public url
     */
    public String getPublicRegistryAddress() {
        List<Route> items = ocp.routes().inNamespace(project).withLabel("apicur.io/name=", registry.getMetadata().getName()).list().getItems();
        if (items.isEmpty()) {
            throw new IllegalStateException("No route for registry '" + registry.getMetadata().getName() + "'");
        }
        String host = items.get(0).getSpec().getHost();
        return "http://" + host;
    }

    private Service getRegistryService() {
        List<Service> items = ocp.services().inNamespace(project).withLabel("apicur.io/name=", registry.getMetadata().getName()).list().getItems();
        if (items.isEmpty()) {
            throw new IllegalStateException("No service for registry '" + registry.getMetadata().getName() + "'");
        }

        return items.get(0);
    }

    /**
     * Undeploy this registry by deleted related ApicurioRegistry CR
     * @return true if the CR was found and deleted
     */
    public boolean undeployRegistry() {
        CustomResourceDefinition crd = ocp.customResourceDefinitions().load(RegistryDeployer.class.getResourceAsStream("/apicur.io_apicurioregistries_crd.yaml")).get();
        return ocp.customResources(crd, ApicurioRegistry.class, ApicurioRegistryList.class, DoneableApicurioRegistry.class).inNamespace(project).delete(registry);
    }
}

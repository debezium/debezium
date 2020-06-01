/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.YAML;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public abstract class DatabaseDeployer<T extends DatabaseDeployer, C extends DatabaseController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseDeployer.class);

    private final OpenShiftClient ocp;
    private final OpenShiftUtils ocpUtils;
    private final String dbType;
    private String project;
    private Deployment deployment;
    private List<Service> services;

    public DatabaseDeployer(String dbType, OpenShiftClient ocp) {
        this.dbType = dbType;
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    public T withProject(String project) {
        this.project = project;
        return getThis();
    }

    public T withDeployment(String yamlPath) {
        return withDeployment(YAML.fromResource(yamlPath, Deployment.class));
    }

    public T withDeployment(Deployment deployment) {
        this.deployment = deployment;
        return getThis();
    }

    public T withServices(String... yamlPath) {
        List<Service> services = Arrays.stream(yamlPath)
                .map(p -> YAML.fromResource(p, Service.class)).collect(Collectors.toList());
        return withServices(services);
    }

    public T withServices(Collection<Service> services) {
        this.services = new ArrayList<>(services);
        return getThis();
    }

    public C deploy() {
        if (deployment == null) {
            throw new IllegalStateException("Deployment configuration not available");
        }
        LOGGER.info("Deploying database");
        Deployment dep = ocp.apps().deployments().inNamespace(project).createOrReplace(deployment);

        List<Service> svcs = services.stream()
                .map(s -> ocp.services().inNamespace(project).createOrReplace(s))
                .collect(Collectors.toList());

        ocpUtils.waitForPods(project, dep.getMetadata().getLabels());
        LOGGER.info("Database deployed successfully");

        this.deployment = dep;
        this.services = svcs;

        return getController(dep, services, dbType, ocp);
    }

    public abstract T getThis();

    public abstract C getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp);
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import static io.debezium.testing.openshift.tools.WaitConditions.scaled;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public abstract class DatabaseController<C extends DatabaseClient<?, ?>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseController.class);

    protected final OpenShiftClient ocp;
    protected final String project;
    protected final String dbType;
    protected final OpenShiftUtils ocpUtils;
    protected Deployment deployment;
    protected String name;
    protected List<Service> services;

    public DatabaseController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        this.deployment = deployment;
        this.name = deployment.getMetadata().getName();
        this.project = deployment.getMetadata().getNamespace();
        this.services = services;
        this.ocp = ocp;
        this.dbType = dbType;
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    public String getDatabaseUrl() {
        Service svc = ocp
                .services()
                .inNamespace(project)
                .withName(deployment.getMetadata().getName() + "-lb")
                .get();
        LoadBalancerIngress ingress = svc.getStatus().getLoadBalancer().getIngress().get(0);
        String hostname = ingress.getHostname();
        Integer port = svc.getSpec().getPorts().stream().filter(p -> p.getName().equals("db")).findAny().get().getPort();
        return constructDatabaseUrl(hostname, port);
    }

    public void reload() throws InterruptedException {
        LOGGER.info("Recreating all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.pods().inNamespace(project).withLabel("deployment", name).delete();
        deployment = ocp.apps().deployments()
                .inNamespace(project)
                .withName(name)
                .waitUntilCondition(WaitConditions::deploymentAvailableCondition, scaled(1), TimeUnit.MINUTES);
        LOGGER.info("Deployment '" + name + "' is available");
        initialize();
    }

    public abstract void initialize() throws InterruptedException;

    public abstract C getDatabaseClient(String username, String password);

    protected abstract String constructDatabaseUrl(String hostname, int port);
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public abstract class AbstractOcpDatabaseController<C extends DatabaseClient<?, ?>>
        implements DatabaseController<C> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOcpDatabaseController.class);

    protected final OpenShiftClient ocp;
    protected final String project;
    protected final OpenShiftUtils ocpUtils;
    protected Deployment deployment;
    protected String name;
    protected List<Service> services;

    public AbstractOcpDatabaseController(
                                         Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        this.deployment = deployment;
        this.name = deployment.getMetadata().getName();
        this.project = deployment.getMetadata().getNamespace();
        this.services = services;
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    private Service getService() {
        return ocp
                .services()
                .inNamespace(project)
                .withName(deployment.getMetadata().getName())
                .get();
    }

    @Override
    public void reload() throws InterruptedException {
        LOGGER.info("Removing all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocpUtils.scaleDeploymentToZero(deployment);
        LOGGER.info("Restoring all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.apps().deployments().inNamespace(project).withName(name).scale(1);
    }

    @Override
    public String getDatabaseHostname() {
        return getService().getMetadata().getName() + "." + project + ".svc.cluster.local";
    }

    @Override
    public int getDatabasePort() {
        return getOriginalDatabasePort();
    }

    @Override
    public String getPublicDatabaseHostname() {
        return getDatabaseHostname();
    }

    @Override
    public int getPublicDatabasePort() {
        return getDatabasePort();
    }

    protected void executeInitCommand(Deployment deployment, String... commands) throws InterruptedException {
        ByteArrayOutputStream captureOut = new ByteArrayOutputStream();
        ByteArrayOutputStream captureErr = new ByteArrayOutputStream();
        PrintStream pso = new PrintStream(captureOut);
        PrintStream pse = new PrintStream(captureErr);

        CountDownLatch latch = new CountDownLatch(1);
        String containerName = deployment.getMetadata().getLabels().get("app");
        try (var ignored = ocpUtils.prepareExec(deployment, project, pso, pse)
                .usingListener(new DatabaseInitListener(containerName, latch))
                .exec(commands)) {
            LOGGER.info("Waiting until database is initialized");
            latch.await(WaitConditions.scaled(1), TimeUnit.MINUTES);
        }
    }

    private int getOriginalDatabasePort() {
        return getService().getSpec().getPorts().stream()
                .filter(p -> p.getName().equals("db"))
                .findAny()
                .get().getPort();
    }
}

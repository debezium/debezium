/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.PortForward;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public abstract class AbstractOcpDatabaseController<C extends DatabaseClient<?, ?>>
        implements DatabaseController<C>, PortForwardableDatabaseController {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOcpDatabaseController.class);

    private static final String FORWARDED_HOST = "localhost";
    private static final int MAX_PORT_SEARCH_ATTEMPTS = 20;
    private static final int MIN_PORT = 32768;
    private static final int MAX_PORT = 60999;

    protected final OpenShiftClient ocp;
    protected final String project;
    protected final OpenShiftUtils ocpUtils;
    protected Deployment deployment;
    protected String name;
    protected List<Service> services;
    protected PortForward portForward;

    private int localPort;

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
        if (!isRunningFromOcp()) {
            try {
                closeDatabasePortForwards();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LOGGER.info("Removing all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.apps().deployments().inNamespace(project).withName(name).scale(0);
        ocpUtils.waitForPodsDeletion(project, deployment);
        LOGGER.info("Restoring all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.apps().deployments().inNamespace(project).withName(name).scale(1);
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
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
        if (isRunningFromOcp()) {
            return getDatabaseHostname();
        }
        return FORWARDED_HOST;
    }

    @Override
    public int getPublicDatabasePort() {
        if (isRunningFromOcp()) {
            return getDatabasePort();
        }
        return localPort;
    }

    @Override
    public void initialize() throws InterruptedException {
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
    }

    @Override
    public void forwardDatabasePorts() {
        if (portForward != null) {
            LOGGER.warn("Calling port forward when forward already on " + getOriginalDatabasePort() + "->" + localPort);
            return;
        }
        String serviceName = getService().getMetadata().getName();
        ServiceResource<Service> serviceResource = ocp.services().inNamespace(project).withName(serviceName);
        int dbPort = getOriginalDatabasePort();
        try {
            localPort = getAvailablePort();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Forwarding ports " + dbPort + "->" + localPort + " on service: " + serviceName);

        PortForward forward = serviceResource
                .portForward(dbPort, localPort);

        for (Throwable e : forward.getClientThrowables()) {
            LOGGER.error("Client error when forwarding DB port " + deployment, e);
        }

        for (Throwable e : forward.getServerThrowables()) {
            LOGGER.error("Server error when forwarding DB port" + dbPort, e);
        }
        portForward = forward;
    }

    @Override
    public void closeDatabasePortForwards() throws IOException {
        LOGGER.info("Closing port forwards");
        portForward.close();
        portForward = null;
    }

    protected void executeInitCommand(String... commands) throws InterruptedException {
        ocpUtils.executeOnPod(name,
                deployment.getMetadata().getLabels().get("app"),
                project,
                "Waiting until database is initialized",
                commands);
    }

    private int getOriginalDatabasePort() {
        return getService().getSpec().getPorts().stream()
                .filter(p -> p.getName().equals("db"))
                .findAny()
                .get().getPort();
    }

    private int getAvailablePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

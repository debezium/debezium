/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.LinkedList;
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

    protected final OpenShiftClient ocp;
    protected final String project;
    protected final OpenShiftUtils ocpUtils;
    protected Deployment deployment;
    protected String name;
    protected List<Service> services;
    protected List<PortForward> portForwards = new LinkedList<>();

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
        ocp.apps().deployments().inNamespace(project).withName(name).scale(0);
        await()
                .atMost(scaled(30), SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).list().getItems().isEmpty());
        LOGGER.info("Restoring all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.apps().deployments().inNamespace(project).withName(name).scale(1);
    }

    @Override
    public String getDatabaseHostname() {
        return getService().getMetadata().getName() + "." + project + ".svc.cluster.local";
    }

    @Override
    public int getDatabasePort() {
        return getService().getSpec().getPorts().stream()
                .filter(p -> p.getName().equals("db"))
                .findAny()
                .get().getPort();
    }

    @Override
    public String getPublicDatabaseHostname() {
        if (isRunningFromOcp()) {
            LOGGER.info("Running from OCP, using internal database hostname");
            return getDatabaseHostname();
        }
        return FORWARDED_HOST;
    }

    @Override
    public int getPublicDatabasePort() {
        return getDatabasePort();
    }

    @Override
    public void forwardDatabasePorts() {
        String dbName = deployment.getMetadata().getLabels().get("app");
        ServiceResource<Service> serviceResource = ocp.services().inNamespace(project).withName(dbName);

        serviceResource.get().getSpec().getPorts().forEach(port -> {
            int servicePort = port.getPort();
            PortForward forward = serviceResource
                    .portForward(servicePort, servicePort);

            for (Throwable e : forward.getClientThrowables()) {
                LOGGER.error("Client error when forwarding DB port " + servicePort, e);
            }

            for (Throwable e : forward.getServerThrowables()) {
                LOGGER.error("Server error when forwarding DB port" + servicePort, e);
            }
            portForwards.add(forward);
        });

        LOGGER.info("Forwarding ports on service: " + dbName);
    }

    @Override
    public void closeDatabasePortForwards() throws IOException {
        LOGGER.info("Closing port forwards");
        for (PortForward portForward : portForwards) {
            portForward.close();
        }
    }
}

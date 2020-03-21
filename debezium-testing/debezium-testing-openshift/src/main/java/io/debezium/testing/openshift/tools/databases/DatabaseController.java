/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class DatabaseController {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseController.class);

    private final OpenShiftClient ocp;
    private final String project;
    private final String dbType;
    private final OpenShiftUtils ocpUtils;
    private Deployment deployment;
    private String name;
    private List<Service> services;

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

        return "jdbc:" + dbType + "://" + hostname + ":" + port + "/";
    }

    public void executeStatement(String database, String username, String password, String sql) throws SQLException {
        try (Connection con = DriverManager.getConnection(getDatabaseUrl(), username, password)) {
            con.setCatalog(database);
            Statement stmt = con.createStatement();
            stmt.execute(sql);
        }
    }

    public void reload() throws InterruptedException {
        LOGGER.info("Recreating all pods of '" + name + "' deployment in namespace '" + project + "'");
        ocp.pods().inNamespace(project).withLabel("deployment", name).delete();
        deployment = ocp.apps().deployments()
                .inNamespace(project)
                .withName(name)
                .waitUntilCondition(this::deploymentAvailableCondition, 30, TimeUnit.SECONDS);
        LOGGER.info("Deployment '" + name + "' is available");
    }

    private boolean deploymentAvailableCondition(Deployment d) {
        return d.getStatus() != null &&
                d.getStatus().getConditions().stream().anyMatch(c -> c.getType().equalsIgnoreCase("Available") && c.getStatus().equalsIgnoreCase("true"));
    }
}

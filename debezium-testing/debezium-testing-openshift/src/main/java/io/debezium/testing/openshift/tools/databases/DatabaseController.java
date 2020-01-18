/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import java.util.List;

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

    private final Deployment deployment;
    private final List<Service> services;
    private final OpenShiftClient ocp;
    private final String project;
    private final String dbType;
    private final OpenShiftUtils ocpUtils;

    public DatabaseController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        this.deployment = deployment;
        this.services = services;
        this.ocp = ocp;
        this.project = deployment.getMetadata().getNamespace();
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

        return "jdbc:" + dbType + "://" + hostname + ":" + port;
    }
}

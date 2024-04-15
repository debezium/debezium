/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import io.fabric8.kubernetes.client.dsl.internal.BaseOperation;
import org.testcontainers.lifecycle.Startable;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Abstraction for a single mongo machine that you can start, stop and connect to using hostname
 */
public class OcpMongoDeploymentManager implements Startable {
    private Deployment deployment;
    private Service service;
    private final String serviceUrl;
    private final OpenShiftClient ocp;
    private final OpenShiftUtils ocpUtils;
    private final String project;

    public OcpMongoDeploymentManager(Deployment deployment, Service service, String serviceUrl, OpenShiftClient ocp, String project) {
        this.deployment = deployment;
        this.service = service;
        this.serviceUrl = serviceUrl;
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.project = project;
    }

    @Override
    public void start() {
//        ocp.resource(deployment).delete();
        deployment = ocp.resource(deployment).serverSideApply();
        service = ocp.resource(service).serverSideApply();
    }

    @Override
    public void stop() {
        ocpUtils.scaleDeploymentToZero(deployment);
    }

    public String getHostname() {
        return service.getMetadata().getName() + "." + project + ".svc.cluster.local";
    }

    public Deployment getDeployment() {
        return deployment;
    }

    public Service getService() {
        return service;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public OpenShiftClient getOcp() {
        return ocp;
    }

    public OpenShiftUtils getOcpUtils() {
        return ocpUtils;
    }

    public String getProject() {
        return project;
    }
}

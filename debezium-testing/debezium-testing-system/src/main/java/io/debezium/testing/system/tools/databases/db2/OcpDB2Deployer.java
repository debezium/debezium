/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.db2;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public class OcpDB2Deployer extends AbstractOcpDatabaseDeployer<OcpDB2Controller> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpDB2Deployer.class);

    private OcpDB2Deployer(
                           String project,
                           Deployment deployment,
                           Secret pullSecret,
                           List<Service> services,
                           OpenShiftClient ocp) {
        super(project, deployment, services, pullSecret, ocp);
    }

    @Override
    public OcpDB2Controller getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpDB2Controller(deployment, services, ocp);
    }

    public static class Builder extends DatabaseBuilder<Builder, OcpDB2Deployer> {
        @Override
        public OcpDB2Deployer build() {
            return new OcpDB2Deployer(
                    project,
                    deployment,
                    pullSecret,
                    services,
                    ocpClient);
        }
    }
}

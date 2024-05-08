/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public class OcpMongoDeployer extends AbstractOcpDatabaseDeployer<OcpMongoController> {

    public static class Deployer extends DatabaseBuilder<OcpMongoDeployer.Deployer, OcpMongoDeployer> {
        @Override
        public OcpMongoDeployer build() {
            return new OcpMongoDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoDeployer.class);

    private OcpMongoDeployer(
                             String project,
                             Deployment deployment,
                             List<Service> services,
                             OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    @Override
    protected OcpMongoController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMongoController(deployment, services, ocp);
    }
}

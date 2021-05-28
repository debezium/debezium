/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.db2;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.DatabaseDeployer;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class DB2Deployer extends DatabaseDeployer<DB2Controller> {

    public static class Deployer extends DatabaseBuilder<DB2Deployer.Deployer, DB2Deployer> {
        @Override
        public DB2Deployer build() {
            return new DB2Deployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DB2Deployer.class);

    private DB2Deployer(
                        String project,
                        Deployment deployment,
                        List<Service> services,
                        OpenShiftClient ocp) {
        super("db2", project, deployment, services, ocp);
    }

    @Override
    public DB2Controller getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new DB2Controller(deployment, services, dbType, ocp);
    }
}

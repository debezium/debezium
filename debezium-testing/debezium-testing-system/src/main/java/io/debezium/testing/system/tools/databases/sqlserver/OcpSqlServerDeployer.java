/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.sqlserver;

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
public class OcpSqlServerDeployer extends AbstractOcpDatabaseDeployer<OcpSqlServerController> {

    public static class Deployer extends DatabaseBuilder<OcpSqlServerDeployer.Deployer, OcpSqlServerDeployer> {
        @Override
        public OcpSqlServerDeployer build() {
            return new OcpSqlServerDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpSqlServerDeployer.class);

    private OcpSqlServerDeployer(
                                 String project,
                                 Deployment deployment,
                                 List<Service> services,
                                 OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    @Override
    public OcpSqlServerController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpSqlServerController(deployment, services, "sqlserver", ocp);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mysql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public final class OcpMySqlDeployer extends AbstractOcpDatabaseDeployer<OcpSqlDatabaseController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMySqlDeployer.class);

    private OcpMySqlDeployer(
                             String project,
                             Deployment deployment,
                             List<Service> services,
                             OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    @Override
    public OcpSqlDatabaseController getController(
                                                  Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpSqlDatabaseController(deployment, services, "mysql", ocp);
    }

    public static class Deployer extends DatabaseBuilder<Deployer, OcpMySqlDeployer> {
        @Override
        public OcpMySqlDeployer build() {
            return new OcpMySqlDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }
}

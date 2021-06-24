/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.postgresql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.AbstractOcpDatabaseDeployer;
import io.debezium.testing.openshift.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public class OcpPostgreSqlDeployer extends AbstractOcpDatabaseDeployer<OcpSqlDatabaseController> {

    public static class Deployer extends DatabaseBuilder<OcpPostgreSqlDeployer.Deployer, OcpPostgreSqlDeployer> {
        @Override
        public OcpPostgreSqlDeployer build() {
            return new OcpPostgreSqlDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpPostgreSqlDeployer.class);

    public OcpPostgreSqlDeployer(
                                 String project,
                                 Deployment deployment,
                                 List<Service> services,
                                 OpenShiftClient ocp) {
        super(project, deployment, services, ocp);
    }

    public OcpSqlDatabaseController getController(
                                                  Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpSqlDatabaseController(deployment, services, "postgresql", ocp);
    }
}

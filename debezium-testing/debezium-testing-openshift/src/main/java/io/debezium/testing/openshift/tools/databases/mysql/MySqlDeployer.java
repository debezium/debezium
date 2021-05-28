/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.mysql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.DatabaseDeployer;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public final class MySqlDeployer extends DatabaseDeployer<SqlDatabaseController> {

    public static class Deployer extends DatabaseBuilder<Deployer, MySqlDeployer> {
        @Override
        public MySqlDeployer build() {
            return new MySqlDeployer(
                    project,
                    deployment,
                    services,
                    ocpClient);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlDeployer.class);

    private MySqlDeployer(
                          String project,
                          Deployment deployment,
                          List<Service> services,
                          OpenShiftClient ocp) {
        super("mysql", project, deployment, services, ocp);
    }

    public SqlDatabaseController getController(
                                               Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new SqlDatabaseController(deployment, services, dbType, ocp);
    }
}

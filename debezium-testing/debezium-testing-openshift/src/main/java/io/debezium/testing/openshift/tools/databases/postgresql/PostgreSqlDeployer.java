/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.postgresql;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.databases.DatabaseDeployer;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class PostgreSqlDeployer extends DatabaseDeployer<PostgreSqlDeployer, SqlDatabaseController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlDeployer.class);

    public PostgreSqlDeployer(OpenShiftClient ocp) {
        super("postgresql", ocp);
    }

    public PostgreSqlDeployer getThis() {
        return this;
    }

    public SqlDatabaseController getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new SqlDatabaseController(deployment, services, dbType, ocp);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class SqlDatabaseController extends DatabaseController<SqlDatabaseClient> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDatabaseController.class);

    public SqlDatabaseController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
    }

    protected String constructDatabaseUrl(String hostname, int port) {
        return "jdbc:" + dbType + "://" + hostname + ":" + port + "/";
    }

    @Override
    public void initialize() throws InterruptedException {
        // no-op
    }

    @Override
    public SqlDatabaseClient getDatabaseClient(String username, String password) {
        String databaseUrl = getDatabaseUrl();
        LOGGER.info("Creating SQL database client for '" + databaseUrl + "'");
        LOGGER.info("Using credentials '" + username + "' / '" + password + "'");
        return new SqlDatabaseClient(databaseUrl, username, password);
    }

}

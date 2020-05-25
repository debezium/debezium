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
 * @author Jakub Cechacek
 */
public class MySqlDeployer extends DatabaseDeployer<MySqlDeployer, DatabaseController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlDeployer.class);

    public MySqlDeployer(OpenShiftClient ocp) {
        super("mysql", ocp);
    }

    public MySqlDeployer getThis() {
        return this;
    }

    public DatabaseController getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new DatabaseController(deployment, services, dbType, ocp);
    }
}

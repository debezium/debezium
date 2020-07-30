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
public class DB2Deployer extends DatabaseDeployer<DB2Deployer, DB2Controller> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DB2Deployer.class);

    public DB2Deployer(OpenShiftClient ocp) {
        super("db2", ocp);
    }

    public DB2Deployer getThis() {
        return this;
    }

    @Override
    public DB2Controller getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new DB2Controller(deployment, services, dbType, ocp);
    }
}

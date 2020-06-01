/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.mongodb;

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
public class MongoDeployer extends DatabaseDeployer<MongoDeployer, MongoController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDeployer.class);

    public MongoDeployer(OpenShiftClient ocp) {
        super("mongo", ocp);
    }

    public MongoDeployer getThis() {
        return this;
    }

    public MongoController getController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        return new MongoController(deployment, services, dbType, ocp);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class PostgreSqlDeployer extends DatabaseDeployer<PostgreSqlDeployer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSqlDeployer.class);

    public PostgreSqlDeployer(OpenShiftClient ocp) {
        super("postgresql", ocp);
    }

    public PostgreSqlDeployer getThis() {
        return this;
    }
}

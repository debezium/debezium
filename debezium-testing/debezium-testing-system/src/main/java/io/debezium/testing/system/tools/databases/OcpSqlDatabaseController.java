/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.mysql.MySqlReplicaController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class OcpSqlDatabaseController
        extends AbstractOcpDatabaseController<SqlDatabaseClient>
        implements SqlDatabaseController, MySqlReplicaController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpSqlDatabaseController.class);

    protected final String dbType;

    public OcpSqlDatabaseController(
                                    Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, ocp);
        this.dbType = dbType;
    }

    @Override
    public String getPublicDatabaseUrl() {
        return "jdbc:" + getDatabaseType() + "://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort() + "/";
    }

    protected String getDatabaseType() {
        return dbType;
    }
}

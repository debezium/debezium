/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.databases.db2;

import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_DB2_DBZ_PASSWORD;
import static io.debezium.testing.openshift.tools.ConfigProperties.DATABASE_DB2_DBZ_USERNAME;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseClient;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class DB2Controller extends SqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DB2Controller.class);
    private static final String READINESS_SQL_SELECT = "SELECT 1 FROM DB2INST1.CUSTOMERS;";

    public DB2Controller(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
    }

    public void initialize() {
        LOGGER.info("Waiting until DB2 instance is ready");
        SqlDatabaseClient client = getDatabaseClient(DATABASE_DB2_DBZ_USERNAME, DATABASE_DB2_DBZ_PASSWORD);
        try {
            client.execute(READINESS_SQL_SELECT);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String constructDatabaseUrl(String hostname, int port) {
        return super.constructDatabaseUrl(hostname, port) + ConfigProperties.DATABASE_DB2_DBZ_DBNAME;
    }

    private String getLog(String podName) {
        return ocp.pods().inNamespace(project).withName(podName).getLog();
    }
}

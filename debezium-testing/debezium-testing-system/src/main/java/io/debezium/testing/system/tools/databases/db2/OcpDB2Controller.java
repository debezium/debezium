/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.db2;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_DBZ_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_DB2_DBZ_USERNAME;

import java.sql.Connection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 *
 * @author Jakub Cechacek
 */
public class OcpDB2Controller extends OcpSqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpDB2Controller.class);
    private static final String READINESS_SQL_SELECT = "SELECT 1 FROM DB2INST1.CUSTOMERS;";

    public OcpDB2Controller(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, "db2", ocp);
    }

    @Override
    public void initialize() {
        LOGGER.info("Waiting until DB2 instance is ready");
        SqlDatabaseClient client = getDatabaseClient(DATABASE_DB2_DBZ_USERNAME, DATABASE_DB2_DBZ_PASSWORD);
        try (Connection connection = client.connectWithRetries()) {
            LOGGER.info("Database connection established successfully!");
        }
        catch (Throwable e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getPublicDatabaseUrl() {
        return super.getPublicDatabaseUrl() + ConfigProperties.DATABASE_DB2_DBZ_DBNAME;
    }
}

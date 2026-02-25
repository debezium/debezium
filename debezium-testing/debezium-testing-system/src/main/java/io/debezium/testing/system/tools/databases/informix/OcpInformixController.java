/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.informix;

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

public class OcpInformixController extends OcpSqlDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpInformixController.class);
    private static final String READINESS_SQL_SELECT = "SELECT 1 FROM informix.systables;";

    public OcpInformixController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, "informix", ocp);
    }

    @Override
    public void initialize() {
        LOGGER.info("Waiting until Informix instance is ready");
        SqlDatabaseClient client = getDatabaseClient(ConfigProperties.DATABASE_INFORMIX_DBZ_USERNAME, ConfigProperties.DATABASE_INFORMIX_DBZ_PASSWORD);
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
        return "jdbc:informix-sqli://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort() + "/" + ConfigProperties.DATABASE_INFORMIX_DBZ_DBNAME
                + ":INFORMIXSERVER=informix";
    }
}

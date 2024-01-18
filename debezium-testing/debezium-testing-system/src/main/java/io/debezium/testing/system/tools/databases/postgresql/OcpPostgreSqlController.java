/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.postgresql;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_USERNAME;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.databases.OcpSqlDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpPostgreSqlController extends OcpSqlDatabaseController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpPostgreSqlController.class);

    public OcpPostgreSqlController(Deployment deployment, List<Service> services, String dbType, OpenShiftClient ocp) {
        super(deployment, services, dbType, ocp);
    }

    @Override
    public void initialize() throws InterruptedException {
        super.initialize();
        var client = getDatabaseClient(DATABASE_POSTGRESQL_USERNAME, DATABASE_POSTGRESQL_PASSWORD);
        try {
            client.execute(DATABASE_POSTGRESQL_DBZ_DBNAME,
                    "CREATE PUBLICATION dbz_publication FOR table inventory.products, inventory.products_on_hand, inventory.customers, inventory.orders");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

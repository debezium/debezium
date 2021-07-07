/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.fixtures.databases;

import io.debezium.testing.openshift.fixtures.OcpClient;
import io.debezium.testing.openshift.fixtures.TestSetupFixture;
import io.debezium.testing.openshift.tools.ConfigProperties;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;
import io.debezium.testing.openshift.tools.databases.postgresql.OcpPostgreSqlDeployer;

public interface OcpPostgreSql extends TestSetupFixture, SqlDatabaseFixture, OcpClient {

    String DB_DEPLOYMENT_PATH = "/database-resources/postgresql/deployment.yaml";
    String DB_SERVICE_PATH_LB = "/database-resources/postgresql/service-lb.yaml";
    String DB_SERVICE_PATH = "/database-resources/postgresql/service.yaml";

    default void setupDatabase() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        OcpPostgreSqlDeployer deployer = new OcpPostgreSqlDeployer.Deployer()
                .withOcpClient(getOcpClient())
                .withProject(ConfigProperties.OCP_PROJECT_POSTGRESQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                .build();
        SqlDatabaseController controller = deployer.deploy();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }
}

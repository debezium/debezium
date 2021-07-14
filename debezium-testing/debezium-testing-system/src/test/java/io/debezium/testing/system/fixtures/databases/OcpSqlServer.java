/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.sqlserver.OcpSqlServerDeployer;

public interface OcpSqlServer extends TestSetupFixture, SqlDatabaseFixture, OcpClient {

    String DB_DEPLOYMENT_PATH = "/database-resources/sqlserver/deployment.yaml";
    String DB_SERVICE_PATH_LB = "/database-resources/sqlserver/service-lb.yaml";
    String DB_SERVICE_PATH = "/database-resources/sqlserver/service.yaml";

    default void setupDatabase() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        OcpSqlServerDeployer deployer = new OcpSqlServerDeployer.Deployer()
                .withOcpClient(getOcpClient())
                .withProject(ConfigProperties.OCP_PROJECT_SQLSERVER)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                .build();
        SqlDatabaseController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);

    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }
}

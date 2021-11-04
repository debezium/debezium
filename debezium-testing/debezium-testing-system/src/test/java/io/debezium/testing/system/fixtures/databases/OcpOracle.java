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
import io.debezium.testing.system.tools.databases.oracle.OcpOracleDeployer;

public interface OcpOracle extends TestSetupFixture, SqlDatabaseFixture, OcpClient {
    String DB_DEPLOYMENT_PATH = "/database-resources/oracle/deployment.yaml";
    String DB_SERVICE_PATH_LB = "/database-resources/oracle/service-lb.yaml";
    String DB_SERVICE_PATH = "/database-resources/oracle/service.yaml";

    default void setupDatabase() throws Exception {
        Class.forName("oracle.jdbc.OracleDriver");
        OcpOracleDeployer deployer = new OcpOracleDeployer.Builder()
                .withOcpClient(getOcpClient())
                .withProject(ConfigProperties.OCP_PROJECT_ORACLE)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                .withPullSecrets(ConfigProperties.OCP_PULL_SECRET_PATH.get())
                .build();
        SqlDatabaseController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }
}

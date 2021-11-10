/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import static io.debezium.testing.system.tools.ConfigProperties.OCP_PULL_SECRET_PATH;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.db2.OcpDB2Deployer;

public interface OcpDB2 extends TestSetupFixture, SqlDatabaseFixture, OcpClient {

    String DB_DEPLOYMENT_PATH = "/database-resources/db2/deployment.yaml";
    String DB_SERVICE_PATH_LB = "/database-resources/db2/service-lb.yaml";
    String DB_SERVICE_PATH = "/database-resources/db2/service.yaml";

    default void setupDatabase() throws Exception {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        OcpDB2Deployer deployer = new OcpDB2Deployer.Builder()
                .withOcpClient(getOcpClient())
                .withProject(ConfigProperties.OCP_PROJECT_DB2)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                .withPullSecrets(OCP_PULL_SECRET_PATH.get())
                .build();
        SqlDatabaseController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }
}

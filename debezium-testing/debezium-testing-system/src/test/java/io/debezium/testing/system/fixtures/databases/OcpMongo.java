/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoController;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoDeployer;

public interface OcpMongo extends TestSetupFixture, MongoDatabaseFixture, OcpClient {

    String DB_DEPLOYMENT_PATH = "/database-resources/mongodb/deployment.yaml";
    String DB_SERVICE_PATH_LB = "/database-resources/mongodb/service-lb.yaml";
    String DB_SERVICE_PATH = "/database-resources/mongodb/service.yaml";

    default void setupDatabase() throws Exception {
        OcpMongoDeployer deployer = new OcpMongoDeployer.Deployer()
                .withOcpClient(getOcpClient())
                .withProject(ConfigProperties.OCP_PROJECT_MONGO)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH, DB_SERVICE_PATH_LB)
                .build();
        OcpMongoController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }
}

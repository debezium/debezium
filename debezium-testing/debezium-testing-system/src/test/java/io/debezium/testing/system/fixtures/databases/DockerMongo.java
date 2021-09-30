/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.databases.mongodb.DockerMongoDeployer;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;

public interface DockerMongo
        extends TestSetupFixture, MongoDatabaseFixture, DockerNetwork {

    default void setupDatabase() throws Exception {
        DockerMongoDeployer deployer = new DockerMongoDeployer.Builder()
                .withNetwork(getNetwork())
                .build();
        MongoDatabaseController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }

}

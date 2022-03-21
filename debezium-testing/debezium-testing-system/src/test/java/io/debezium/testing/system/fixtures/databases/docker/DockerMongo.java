/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.databases.mongodb.DockerMongoDeployer;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { MongoDatabaseController.class })
public class DockerMongo extends DockerDatabaseFixture<MongoDatabaseController> {

    public DockerMongo(ExtensionContext.Store store) {
        super(MongoDatabaseController.class, store);
    }

    @Override
    protected MongoDatabaseController databaseController() throws Exception {
        DockerMongoDeployer deployer = new DockerMongoDeployer.Builder()
                .withNetwork(network)
                .build();
        return deployer.deploy();
    }
}

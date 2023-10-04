/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DOCKER_DESKTOP_PORTS;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.DockerMongoController;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.util.DockerUtils;
import io.debezium.testing.testcontainers.util.ParsingPortResolver;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { MongoDatabaseController.class })
public class DockerMongo extends DockerDatabaseFixture<MongoDatabaseController> {

    private MongoDbReplicaSet mongo;

    public DockerMongo(ExtensionContext.Store store) {
        super(MongoDatabaseController.class, store);
    }

    @Override
    protected MongoDatabaseController databaseController() throws Exception {
        // Creating a deployer class for this is probably not worth it

        var portResolver = new ParsingPortResolver(DATABASE_MONGO_DOCKER_DESKTOP_PORTS);
        mongo = MongoDbReplicaSet.replicaSet()
                .memberCount(ConfigProperties.DATABASE_MONGO_DOCKER_REPLICA_SIZE)
                .portResolver(portResolver)
                .network(network)
                .authEnabled(true)
                .rootUser(ConfigProperties.DATABASE_MONGO_USERNAME, ConfigProperties.DATABASE_MONGO_SA_PASSWORD)
                .build();

        DockerUtils.enableFakeDnsIfRequired();
        mongo.start();

        return new DockerMongoController(mongo);
    }

    @Override
    public void teardown() throws Exception {
        mongo.stop();
        DockerUtils.disableFakeDns();
    }
}

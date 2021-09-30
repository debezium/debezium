/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import io.debezium.testing.system.tools.databases.AbstractDockerDatabaseController;

public class DockerMongoController
        extends AbstractDockerDatabaseController<MongoDBContainer, MongoDatabaseClient>
        implements MongoDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerMongoController.class);
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/usr/local/bin/init-inventory.sh";

    public static final int MONGODB_INTERNAL_PORT = 27017;

    public DockerMongoController(MongoDBContainer container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return MONGODB_INTERNAL_PORT;
    }

    @Override
    public String getPublicDatabaseUrl() {
        container.getReplicaSetUrl();
        return "mongodb://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort();
    }

    @Override
    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, "admin");
    }

    @Override
    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getPublicDatabaseUrl(), username, password, authSource);
    }

    @Override
    public void initialize() throws InterruptedException {
        LOGGER.info("Waiting until database is initialized");
        try {
            container.execInContainer("bash", "-c", DB_INIT_SCRIPT_PATH_CONTAINER + " -h " + getDatabaseHostname());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

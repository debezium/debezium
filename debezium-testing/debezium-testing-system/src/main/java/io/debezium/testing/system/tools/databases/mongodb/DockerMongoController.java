/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.MountableFile;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.testcontainers.MongoDbReplicaSet;

public class DockerMongoController implements MongoDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerMongoController.class);
    private static final String DB_INIT_SCRIPT_PATH_CONTAINER = "/usr/local/bin/init-inventory.js";

    public static final int MONGODB_INTERNAL_PORT = 27017;
    private static final MountableFile INIT_SCRIPT_RESOURCE = MountableFile.forClasspathResource(
            "/database-resources/mongodb/docker/init-inventory.js");
    private final MongoDbReplicaSet mongo;

    public DockerMongoController(MongoDbReplicaSet mongo) {
        this.mongo = mongo;
    }

    @Override
    public String getDatabaseHostname() {
        throw new UnsupportedOperationException("MongoDB docker controller doesn't support direct access to hostnames");
    }

    @Override
    public int getDatabasePort() {
        return MONGODB_INTERNAL_PORT;
    }

    @Override
    public String getPublicDatabaseHostname() {
        return getDatabaseHostname();
    }

    @Override
    public int getPublicDatabasePort() {
        throw new UnsupportedOperationException("MongoDB docker controller doesn't support direct access to ports");
    }

    @Override
    public String getPublicDatabaseUrl() {
        return mongo.getNoAuthConnectionString();
    }

    @Override
    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, "admin");
    }

    @Override
    public void reload() {
        mongo.stop();
        mongo.start();
    }

    @Override
    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getPublicDatabaseUrl(), username, password, authSource);
    }

    @Override
    public void initialize() throws InterruptedException {
        LOGGER.info("Waiting until database is initialized");
        mongo.execMongoScript(INIT_SCRIPT_RESOURCE, DB_INIT_SCRIPT_PATH_CONTAINER);
        // High privileges in order to avoid custom role creation (no, RBAC doesn't work properly in other tests using example mongo container)
        mongo.createUser(ConfigProperties.DATABASE_MONGO_DBZ_USERNAME, ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD, "admin", "root");
    }
}

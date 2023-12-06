/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentfactories.OcpMongosModelFactory;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.client.OpenShiftClient;

import freemarker.template.TemplateException;
import lombok.Getter;

public class OcpMongoShardedController implements MongoDatabaseController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedController.class);

    @Getter
    private final OcpMongoShardedCluster mongo;
    private final OpenShiftClient ocp;
    private final String project;
    private final Path insertDataScript;

    public OcpMongoShardedController(OcpMongoShardedCluster mongo, OpenShiftClient ocp, String project) {
        this.mongo = mongo;
        this.ocp = ocp;
        this.project = project;
        try {
            insertDataScript = Paths.get(getClass().getResource(OcpMongoShardedConstants.INSERT_MONGOS_DATA_SCRIPT_LOC).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDatabaseHostname() {
        return getService().getMetadata().getName() + "." + project + ".svc.cluster.local";
    }

    @Override
    public int getDatabasePort() {
        return OcpMongoShardedConstants.MONGO_MONGOS_PORT;
    }

    @Override
    public String getPublicDatabaseHostname() {
        return getDatabaseHostname();
    }

    @Override
    public int getPublicDatabasePort() {
        return getDatabasePort();
    }

    @Override
    public String getPublicDatabaseUrl() {
        return mongo.getConnectionString();
    }

    @Override
    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, OcpMongoShardedConstants.ADMIN_DB);
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getPublicDatabaseUrl(), username, password, authSource);
    }

    @Override
    public void reload() {
        mongo.stop();
        mongo.waitForStopped();
    }

    @Override
    public void initialize() throws InterruptedException {
        try {
            // fill test data, create debezium user
            mongo.executeMongoSh(String.join("\n", Files.readAllLines(insertDataScript)));
            mongo.executeMongoSh(MongoShardedUtil.createDebeziumUserCommand(ConfigProperties.DATABASE_MONGO_DBZ_USERNAME, ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD));
        }
        catch (IOException | TemplateException e) {
            throw new RuntimeException(e);
        }

        // each shard has to have debezium user created for replica_set connection type
        mongo.getShardReplicaSets().forEach(rs -> {
            try {
                rs.executeMongosh(MongoShardedUtil.createDebeziumUserCommand(ConfigProperties.DATABASE_MONGO_DBZ_USERNAME, ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD),
                        true);
            }
            catch (IOException | TemplateException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void addShard(Map<MongoShardKey, ShardKeyRange> rangeMap) {
        mongo.addShard(rangeMap);
    }

    /**
     * removes last shard
     */
    public void removeShard() {
        mongo.removeShard();
    }

    private Service getService() {
        return ocp
                .services()
                .inNamespace(project)
                .withName(OcpMongosModelFactory.DEPLOYMENT_NAME)
                .get();
    }

}

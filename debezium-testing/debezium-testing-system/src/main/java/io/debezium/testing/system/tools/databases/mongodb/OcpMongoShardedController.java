/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.debezium.testing.system.tools.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseController;
import io.debezium.testing.system.tools.databases.DatabaseInitListener;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoShardedController extends AbstractOcpDatabaseController<MongoDatabaseClient> implements MongoDatabaseController {
    private ConnectorConnectionMode connectionMode;
    private List<String> shardServices;
    private Service mongosService;
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedController.class);

    public OcpMongoShardedController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, ocp);
    }

    @Override
    public String getPublicDatabaseUrl() {
        return "mongodb://" + getPublicDatabaseHostname() + ":" + getPublicDatabasePort();
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password) {
        return getDatabaseClient(username, password, "admin");
    }

    public MongoDatabaseClient getDatabaseClient(String username, String password, String authSource) {
        return new MongoDatabaseClient(getPublicDatabaseUrl(), username, password, authSource);
    }

    @Override
    public void reload() throws InterruptedException {

    }

    @Override
    public void initialize() throws InterruptedException {
        Pod configPod = ocp.pods().inNamespace(project).withLabel("deployment", "mongo-config").list().getItems().get(0);
        CountDownLatch latch = new CountDownLatch(1);
        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(configPod.getMetadata().getName())
                .inContainer("mongo")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new DatabaseInitListener("mongo", latch))
                .exec("mongod", "--configsvr", "--replSet", "cfgrs", "--port", "27017", "--dbpath", "/data/db")) {
            LOGGER.info("Waiting until database is initialized");
            latch.await(WaitConditions.scaled(1), TimeUnit.MINUTES);
        }

        getDatabaseClient(ConfigProperties.DATABASE_MONGO_DBZ_USERNAME, ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD);

        // TODO init config
        // TODO init shard
        // TODO init mongos
    }

    public String getConnectionModeString() {
        return connectionMode == ConnectorConnectionMode.REPLICA_SET ? "replica_set" : "sharded";
    }

    public String getConnectionUrl() {
        // TODO check
        if (connectionMode == ConnectorConnectionMode.REPLICA_SET) {
            // EXAMPLE mongodb://mongos0.example.com:27017,mongos1.example.com:27017,mongos2.example.com:27017
            return "mongodb://" + services.stream().map(s -> s.getMetadata().getName() + ":" + s.getSpec().getPorts()).collect(Collectors.joining(","));
        }
        else {
            return "mongodb://" + mongosService.getMetadata().getName() + ":" + mongosService.getSpec().getPorts();
        }
    }

    public enum ConnectorConnectionMode {
        REPLICA_SET,
        SHARDED
    }
}

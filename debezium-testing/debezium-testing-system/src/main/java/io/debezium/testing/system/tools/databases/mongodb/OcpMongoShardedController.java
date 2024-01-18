/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoShardedController extends AbstractOcpDatabaseController<MongoDatabaseClient> implements MongoDatabaseController {

    public static final String INIT_MONGOS_SCRIPT_LOCATION = "/database-resources/mongodb/sharded/init-mongos.js";
    public static final String CREATE_DBZ_USER_SCRIPT_LOCATION = "/database-resources/mongodb/sharded/create-dbz-user.js";
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
        // restart every sharded mongo component
        LOGGER.info("Restarting all mongo shards and mongos");
        List<Deployment> deployments = ocp
                .apps()
                .deployments()
                .inNamespace(project)
                .list()
                .getItems()
                .stream()
                .filter(d -> d.getMetadata().getName().equals(OcpMongoShardedConstants.MONGO_CONFIG_DEPLOYMENT_NAME) ||
                        d.getMetadata().getName().equals(OcpMongoShardedConstants.MONGO_MONGOS_DEPLOYMENT_NAME) ||
                        d.getMetadata().getName().startsWith(OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX))
                .collect(Collectors.toList());

        deployments.stream().parallel().forEach(mongoComponent -> {
            Deployment deployment1 = ocp.apps().deployments().inNamespace(project).withName(mongoComponent.getMetadata().getName()).get();
            ocpUtils.scaleDeploymentToZero(deployment1);
            ocp.apps().deployments().inNamespace(project).withName(deployment1.getMetadata().getName()).scale(1);
        });
    }

    public void executeCommandOnComponent(String componentName, String... commands) throws InterruptedException {
        var maybeDeployment = ocpUtils.deploymentsWithPrefix(project, componentName);
        if (maybeDeployment.isEmpty()) {
            throw new IllegalStateException("Deployment of " + componentName + " missing");
        }
        executeCommand(maybeDeployment.get(),
                commands);
    }

    @Override
    public void initialize() throws InterruptedException {
        // init config
        LOGGER.info("Initializing replica-set");
        executeCommandOnComponent("mongo-config",
                "mongosh",
                "localhost:" + OcpMongoShardedConstants.MONGO_CONFIG_PORT,
                "--eval",
                "rs.initiate({ _id: \"cfgrs\", configsvr: true, members: [{ _id : 0, host : \"mongo-config." + ConfigProperties.OCP_PROJECT_MONGO
                        + ".svc.cluster.local:" + OcpMongoShardedConstants.MONGO_CONFIG_PORT + "\" }]})");
        uploadAndExecuteMongoScript(CREATE_DBZ_USER_SCRIPT_LOCATION, "mongo-config", OcpMongoShardedConstants.MONGO_CONFIG_PORT);

        // init shards
        LOGGER.info("Initializing all the shards");
        List<Integer> shardRange = IntStream.rangeClosed(1, OcpMongoShardedConstants.SHARD_COUNT).boxed().collect(Collectors.toList());
        shardRange.parallelStream().forEach(s -> {
            try {
                executeCommandOnComponent("mongo-shard" + s + "r1", getShardInitCommand(s));
                uploadAndExecuteMongoScript(CREATE_DBZ_USER_SCRIPT_LOCATION, OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX + s + "r1",
                        OcpMongoShardedConstants.MONGO_SHARD_PORT);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // init mongos
        LOGGER.info("Adding shards to mongos");
        addShard(1, "ONE", 1000, 1003);
        addShard(2, "TWO", 1003, 1004);

        uploadAndExecuteMongoScript(INIT_MONGOS_SCRIPT_LOCATION, "mongo-mongos", OcpMongoShardedConstants.MONGO_MONGOS_PORT);
    }

    public void addShard(int shardNumber, String zoneName, int rangeStart, int rangeEnd) throws InterruptedException {
        List<Integer> replicaRange = IntStream.rangeClosed(1, OcpMongoShardedConstants.REPLICAS_IN_SHARD).boxed().collect(Collectors.toList());
        executeCommandOnComponent(OcpMongoShardedConstants.MONGO_MONGOS_DEPLOYMENT_NAME, "mongosh",
                "localhost:27017",
                "--eval",
                "sh.addShard(\"shard" + shardNumber + "rs/" + replicaRange.stream().map(r -> getShardReplicaServiceName(shardNumber, r)).collect(Collectors.joining(","))
                        + "\");"
                        +
                        "sh.addShardToZone(\"shard" + shardNumber + "rs\", \"" + zoneName + "\");" +
                        "sh.updateZoneKeyRange(\"inventory.customers\",{ _id : " + rangeStart + " },{ _id : " + rangeEnd + " },\"" + zoneName + "\");");
    }

    public void removeShard(int shardNumber, int rangeStart, int rangeEnd) throws InterruptedException {
        executeCommandOnComponent(OcpMongoShardedConstants.MONGO_MONGOS_DEPLOYMENT_NAME, "mongosh",
                "localhost:27017",
                "--eval",
                "sh.removeRangeFromZone(\"inventory.customers\",{ _id : " + rangeStart + " },{ _id : " + rangeEnd + " });" +
                        "db.adminCommand({removeShard:\"shard" + shardNumber + "rs\"})");
    }

    private void uploadAndExecuteMongoScript(String scriptLocation, String component, int port) throws InterruptedException {
        Path scriptPath;
        try {
            scriptPath = Paths.get(
                    Objects.requireNonNull(
                            getClass().getResource(scriptLocation)).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        String podName = ocpUtils.podsWithLabels(project, Map.of("deployment", component))
                .get(0)
                .getMetadata()
                .getName();

        var podResource = ocp.pods().inNamespace(project).withName(podName);

        // give unique name in case multiple scripts are uploaded to container
        String containerPath = "/opt/" + scriptPath.getFileName().toString() + TestUtils.getUniqueId() + ".js";

        podResource.file(containerPath)
                .upload(scriptPath);
        executeCommandOnComponent(component,
                "mongosh", "localhost:" + port, "-f", containerPath);
    }

    private String[] getShardInitCommand(int shardNum) {
        List<Integer> replicaRange = IntStream.rangeClosed(1, OcpMongoShardedConstants.REPLICAS_IN_SHARD).boxed().collect(Collectors.toList());
        // on replica #1 create replica set for shard and wait for node to become primary
        return new String[]{ "mongosh", "localhost:27018", "--eval",
                "rs.initiate({ _id: \"shard" + shardNum + "rs\", members: [" + replicaRange
                        .stream()
                        .map(replicaNum -> "{_id : " + (replicaNum - 1) + ", host : \"" + getShardReplicaServiceName(shardNum, replicaNum) + "\" }")
                        .collect(Collectors.joining(",")) + "]});" +
                        "let isPrimary = false;\n" +
                        "let count = 0;\n" +
                        "while(isPrimary == false && count < 30) {\n" +
                        "  const rplStatus = db.adminCommand({ replSetGetStatus : 1 });\n" +
                        "  isPrimary = rplStatus.members[0].stateStr === \"PRIMARY\";\n" +
                        "  print(\"is primary result: \", isPrimary);\n" +
                        "  count = count + 1;\n" +
                        "  sleep(1000);\n" +
                        "}" };
    }

    private String getShardReplicaServiceName(int shardNum, int replicaNum) {
        return OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX + shardNum + "r" + replicaNum + "." + ConfigProperties.OCP_PROJECT_MONGO
                + ".svc.cluster.local:27018";
    }

}

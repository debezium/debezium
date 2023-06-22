/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

import lombok.SneakyThrows;

public class OcpMongoShardedController extends AbstractOcpDatabaseController<MongoDatabaseClient> implements MongoDatabaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedController.class);
    private Path initScript;

    public OcpMongoShardedController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        super(deployment, services, ocp);
        try {
            initScript = Paths.get(
                    Objects.requireNonNull(
                            getClass().getResource("/database-resources/mongodb/sharded/init-mongos.js")).toURI());
        }
        catch (URISyntaxException e) {
            LOGGER.error("database-resources/mongodb/sharded/init-mongos.js not found :/");
        }
    }

    private String getShardNumber(MongoComponents shard) {
        var matcher = Pattern.compile("mongo-shard([0-9])r1").matcher(shard.getName());
        if (!matcher.find()) {
            throw new IllegalArgumentException("Mongo component not a shard");
        }
        else {
            return matcher.group(1);
        }
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
        if (!isRunningFromOcp()) {
            try {
                closeDatabasePortForwards();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // restart every mongo component individually
        Arrays.stream(MongoComponents.values()).forEach(mongoComponent -> {
            Deployment deployment1 = ocp.apps().deployments().inNamespace(project).withName(mongoComponent.getName()).get();

            LOGGER.info("Removing all pods of '" + name + "' deployment in namespace '" + project + "'");
            ocp.apps().deployments().inNamespace(project).withName(mongoComponent.getName()).scale(0);
            ocpUtils.waitForPodsDeletion(project, deployment1);
            LOGGER.info("Restoring all pods of '" + name + "' deployment in namespace '" + project + "'");
            ocp.apps().deployments().inNamespace(project).withName(name).scale(1);
        });
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
    }

    @SneakyThrows
    public void executeOnPodOfDeployment(MongoComponents component, String[] commands) {
        ocpUtils.executeOnPod(component.getName(),
                "mongo",
                project,
                "Waiting until database " + component.getName() + " is initialized",
                commands);
    }

    @Override
    public void initialize() throws InterruptedException {
        Arrays.stream(MongoComponents.values()).forEach(c -> {
            if (c != MongoComponents.MONGOS) {
                executeOnPodOfDeployment(c, c.getInitCommand().toArray(String[]::new));
            }
            uploadAndExecuteMongoScript("/database-resources/mongodb/sharded/create-dbz-user.js", c);
        });

        uploadAndExecuteMongoScript("/database-resources/mongodb/sharded/init-mongos.js", MongoComponents.MONGOS);

        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
    }

    public void addShard(MongoComponents shard) {
        String shardNumber = getShardNumber(shard);
        executeOnPodOfDeployment(MongoComponents.MONGOS, new String[]{
                "mongosh",
                "localhost:27017",
                "--eval",
                "sh.addShard(\"shard" + shardNumber + "rs/mongo-shard" + shardNumber + "r1." + project + ".svc.cluster.local:27018\");" +
                        "sh.addShardToZone(\"shard3rs\", \"THREE\");" +
                        "sh.updateZoneKeyRange(\"inventory.customers\",{ _id : 1004 },{ _id : 1005 },\"THREE\");"
        });
    }

    public void removeShard(MongoComponents shard) {

        executeOnPodOfDeployment(MongoComponents.MONGOS, new String[]{
                "mongosh",
                "localhost:27017",
                "--eval",
                "sh.removeRangeFromZone(\"inventory.customers\",{ _id : 1004 },{ _id : 1005 });" +
                        "db.adminCommand({removeShard:\"shard" + getShardNumber(shard) + "rs\"});" +
                        "db.adminCommand({removeShard:\"shard" + getShardNumber(shard) + "rs\"})"
        });

    }

    private void uploadAndExecuteMongoScript(String scriptLocation, MongoComponents component) {
        Path scriptPath;
        try {
            scriptPath = Paths.get(
                    Objects.requireNonNull(
                            getClass().getResource(scriptLocation)).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        var podName = ocpUtils.podsWithLabels(project, Map.of("deployment", component.getName()))
                .get(0)
                .getMetadata()
                .getName();

        var podResource = ocp.pods().inNamespace(project).withName(podName);

        // give unique name in case multiple scripts are uploaded to container
        String containerPath = "/opt/" + scriptPath.getFileName().toString() + TestUtils.getUniqueId() + ".js";

        podResource.file(containerPath)
                .upload(scriptPath);
        executeOnPodOfDeployment(component,
                new String[]{ "mongosh", "localhost:" + component.getPort(), "-f", containerPath });
    }

    public enum MongoComponents {
        CONFIG("mongo-config", 27019, List.of("mongosh", "localhost:27019", "--eval",
                "rs.initiate({ _id: \"cfgrs\", configsvr: true, members: [{ _id : 0, host : \"mongo-config." + ConfigProperties.OCP_PROJECT_MONGO
                        + ".svc.cluster.local:27019\" }]})")),
        SHARD1R1("mongo-shard1r1", 27018, List.of("mongosh", "localhost:27018", "--eval",
                "rs.initiate({_id: \"shard1rs\", members: [{ _id : 0, host : \"mongo-shard1r1." + ConfigProperties.OCP_PROJECT_MONGO
                        + ".svc.cluster.local:27018\" }]})")),
        SHARD2R1("mongo-shard2r1", 27018, List.of("mongosh", "localhost:27018", "--eval",
                "rs.initiate({_id: \"shard2rs\", members: [{ _id : 0, host : \"mongo-shard2r1." + ConfigProperties.OCP_PROJECT_MONGO
                        + ".svc.cluster.local:27018\" }]})")),
        SHARD3R1("mongo-shard3r1", 27018, List.of("mongosh", "localhost:27018", "--eval",
                "rs.initiate({_id: \"shard3rs\", members: [{ _id : 0, host : \"mongo-shard3r1." + ConfigProperties.OCP_PROJECT_MONGO
                        + ".svc.cluster.local:27018\" }]})")),
        MONGOS("mongo-mongos", 27017, null);

        private final String name;
        private final int port;
        private final List<String> initCommand;

        MongoComponents(String name, int port, List<String> initCommand) {
            this.name = name;
            this.port = port;
            this.initCommand = initCommand;
        }

        public String getName() {
            return name;
        }

        public int getPort() {
            return port;
        }

        public List<String> getInitCommand() {
            return initCommand;
        }
    }
}

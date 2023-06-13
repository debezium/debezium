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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseController;
import io.debezium.testing.system.tools.databases.DatabaseInitListener;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
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

    public String getReplicaSetUrl() {
        var urlList = Arrays.stream(MongoComponents.values())
                .map(c -> {
                    // only match primary from each replica set
                    var matcher = Pattern.compile("mongo-shard([0-9])r1").matcher(c.getName());
                    if (!matcher.find()) {
                        return null;
                    } // "shard" + matcher.group(1) + "/" +
                    return serviceHostnameFromName(c.getName()) + ":" + c.getPort();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return "mongodb://" + String.join(",", urlList);
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
    private void executeOnPod(String deploymentName, String[] commands) {
        Pod configPod = ocpUtils.podsWithLabels(project, Map.of("deployment", deploymentName)).get(0);
        CountDownLatch latch = new CountDownLatch(1);
        try (ExecWatch exec = ocp.pods().inNamespace(project).withName(configPod.getMetadata().getName())
                .inContainer("mongo")
                .writingOutput(System.out) // CHECKSTYLE IGNORE RegexpSinglelineJava FOR NEXT 2 LINES
                .writingError(System.err)
                .usingListener(new DatabaseInitListener("mongo", latch))
                .exec(commands)) {
            LOGGER.info("Waiting until database " + deploymentName + " is initialized");
            latch.await(WaitConditions.scaled(1), TimeUnit.MINUTES);
        }
    }

    @Override
    public void initialize() throws InterruptedException {
        Arrays.stream(MongoComponents.values()).forEach(c -> {
            executeOnPod(c.getName(), c.getInitCommand().toArray(String[]::new));
            try {
                uploadAndExecuteMongoScript("/database-resources/mongodb/sharded/create-dbz-user.js", c);
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        });

        var mongosPod = ocp.pods().inNamespace(project).withLabel("deployment", "mongo-mongos").list().getItems().get(0);
        var mongosPodRes = ocp.pods().inNamespace(project).withName(mongosPod.getMetadata().getName());

        String scriptLocationInContainer = "/opt/init-mongos.js";
        mongosPodRes.file(scriptLocationInContainer)
                .upload(initScript);
        executeOnPod("mongo-mongos",
                new String[]{ "mongosh", "localhost:27017", "-f", scriptLocationInContainer });
        if (!isRunningFromOcp()) {
            forwardDatabasePorts();
        }
    }

    private void uploadAndExecuteMongoScript(String scriptLocation, MongoComponents component) throws URISyntaxException {
        Path scriptPath = Paths.get(
                Objects.requireNonNull(
                        getClass().getResource(scriptLocation)).toURI());
        var podName = ocpUtils.podsWithLabels(project, Map.of("deployment", component.getName()))
                .get(0)
                .getMetadata()
                .getName();

        var podResource = ocp.pods().inNamespace(project).withName(podName);

        String containerPath = "/opt/script" + TestUtils.getUniqueId() + ".js";

        podResource.file(containerPath)
                .upload(scriptPath);
        executeOnPod(component.getName(),
                new String[]{ "mongosh", "localhost:" + component.getPort(), "-f", containerPath });
    }

    public enum MongoComponents {
        CONFIG("mongo-config", 27019, List.of("mongosh", "localhost:27019", "--eval",
                "rs.initiate({ _id: \"cfgrs\", configsvr: true, members: [{ _id : 0, host : \"mongo-config:27019\" }]})")),
        SHARD1R1("mongo-shard1r1", 27018, List.of("mongosh", "localhost:27018", "--eval",
                "rs.initiate({_id: \"shard1rs\", members: [{ _id : 0, host : \"mongo-shard1r1:27018\" }]})")),
        SHARD2R1("mongo-shard2r1", 27018, List.of("mongosh", "localhost:27018", "--eval",
                "rs.initiate({_id: \"shard2rs\", members: [{ _id : 0, host : \"mongo-shard2r1:27018\" }]})")),
        MONGOS("mongo-mongos", 27017, List.of("mongosh", "localhost:27017", "--eval",
                "rs.initiate({_id: \"mgs\", members: [{ _id : 0, host : \"mongo-mongos:27017\" }]})"));

        private String name;
        private int port;
        private List<String> initCommand;

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

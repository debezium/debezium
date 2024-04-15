/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static io.debezium.testing.system.tools.databases.mongodb.sharded.MongoShardedUtil.executeMongoShOnPod;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startable;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentproviders.OcpMongosModelProvider;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentproviders.OcpShardModelProvider;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

import freemarker.template.TemplateException;

/**
 * Mongo sharded cluster containing config server replica set, one or more shard replica sets and a mongos router
 */
public class OcpMongoShardedCluster implements Startable {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedCluster.class);
    private final int replicaCount;
    private final int configServerCount;
    private final String rootUserName;
    private final String rootPassword;
    private final boolean useInternalAuth;
    private final boolean useTls;
    private final OpenShiftClient ocp;
    private final OpenShiftUtils ocpUtils;
    private final int initialShardCount;
    private final String project;
    private final List<MongoShardKey> shardKeys;
    private final List<OcpMongoReplicaSet> shardReplicaSets = Collections.synchronizedList(new LinkedList<>());
    private OcpMongoReplicaSet configServerReplicaSet;
    private OcpMongoDeploymentManager mongosRouter;
    private boolean isRunning = false;

    /**
     * Deploy all deployments and services to openshift, initialize replicaSets and sharding, create root user
     */
    @Override
    public void start() {
        if (isRunning) {
            LOGGER.info("Sharded mongo cluster already running, skipping initialization");
            return;
        }

        if (useTls && useInternalAuth) {
            throw new IllegalStateException("Cannot deploy mongo with both tls and keyfile internal auth");
        }

        if (useInternalAuth) {
            String internalKey;
            try {
                Path keyFile = Paths.get(getClass().getResource("/database-resources/mongodb/mongodb.keyfile").toURI());
                internalKey = Files.readString(keyFile);
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }

            LOGGER.info("creating keyfile configmap in " + project);
            ConfigMap map = new ConfigMapBuilder()
                    .withMetadata(new ObjectMetaBuilder()
                            .withName(OcpMongoShardedConstants.KEYFILE_CONFIGMAP_NAME)
                            .build())
                    .withData(Map.of("mongodb.keyfile", internalKey))
                    .build();
            ocp.resource(map).create();
        }

        // deploy mongo components
        deployConfigServers();
        deployShards();
        deployMongos();
        ocpUtils.waitForPods(project, mongosRouter.getDeployment().getMetadata().getLabels());

        // initialize sharding
        try {
            initMongos();
        }
        catch (IOException | TemplateException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        isRunning = true;
    }

    /**
     * Scale all cluster deployments to zero
     */
    @Override
    public void stop() {
        shardReplicaSets.parallelStream().forEach(OcpMongoReplicaSet::stop);
        configServerReplicaSet.stop();
        mongosRouter.stop();
        isRunning = false;
    }

    /**
     * delete last added shard
     */
    public void removeShard() {
        int shardNum = shardReplicaSets.size() - 1;
        var rs = shardReplicaSets.get(shardNum);
        LOGGER.info("Removing shard " + (shardNum));

        shardKeys.forEach(k -> {
            var keyRanges = k.getKeyRanges().stream().filter(r -> r.getShardName().equals(rs.getName())).collect(Collectors.toList());
            keyRanges.forEach(r -> executeMongoSh(
                    String.format("sh.removeRangeFromZone(\"%s\", {%s : %s}, {%s : %s})\n", k.getCollection(), k.getKey(), r.getStart(), k.getKey(), r.getEnd())));
        });

        executeMongoSh(String.format("sh.removeShardFromZone(\"%s\",\"%s\");", rs.getName(), rs.getName()));
        // call mongos removeShard command and wait until chunk cleanup is done and shard can be safely deleted
        await()
                .atMost(scaled(20), TimeUnit.MINUTES)
                .pollInterval(20, TimeUnit.SECONDS)
                .until(() -> {
                    var outputs = executeMongoSh(String.format("db.adminCommand( { removeShard: \"%s\" } )", rs.getName()));
                    return outputs.getStdOut().contains("state: 'completed'");
                });
        rs.stop();
        shardReplicaSets.remove(rs);
    }

    /**
     * deploy new shard and initialize it. Requires running initialized sharded mongo cluster
     */
    public OcpMongoReplicaSet addShard(@Nullable Map<MongoShardKey, ShardKeyRange> rangeMap) {
        int shardNum = shardReplicaSets.size();
        var rs = deployNewShard(shardNum);
        registerShardInMongos(rangeMap, rs);
        return rs;
    }

    /**
     * get connection string for mongos router
     * */
    public String getConnectionString() {
        StringBuilder builder = new StringBuilder("mongodb://");
        if (StringUtils.isNotEmpty(rootUserName) && StringUtils.isNotEmpty(rootPassword)) {
            builder.append(rootUserName)
                    .append(":")
                    .append(rootPassword)
                    .append("@");
        }
        builder.append(mongosRouter.getHostname() + ":" + OcpMongoShardedConstants.MONGO_MONGOS_PORT);
        return builder.toString();
    }

    public MongoShardKey getShardKey(String collection) {
        return shardKeys.stream().filter(s -> s.getCollection().equals(collection)).findFirst().get();
    }

    /**
     * execute a mongosh command/script on mongos router
     * @param command
     * @return captured outputs of command execution
     */
    public OpenShiftUtils.CommandOutputs executeMongoSh(String command) {
        return executeMongoShOnPod(ocpUtils, project, mongosRouter.getDeployment(), getConnectionString(), command, false);
    }

    public List<MongoShardKey> getShardKeys() {
        return shardKeys;
    }

    public List<OcpMongoReplicaSet> getShardReplicaSets() {
        return shardReplicaSets;
    }

    public OcpMongoReplicaSet getConfigServerReplicaSet() {
        return configServerReplicaSet;
    }

    private void deployShards() {
        MongoShardedUtil.intRange(initialShardCount).parallelStream().forEach(this::deployNewShard);
    }

    /**
     * deploy new shard, initialize replica set and set authentication if specified
     */
    private OcpMongoReplicaSet deployNewShard(int shardNum) {
        LOGGER.info("Deploying shard number " + shardNum);
        OcpMongoReplicaSet replicaSet = OcpMongoReplicaSet.builder()
                .withShardNum(shardNum)
                .withName(OcpShardModelProvider.getShardReplicaSetName(shardNum))
                .withConfigServer(false)
                .withRootUserName(rootUserName)
                .withRootPassword(rootPassword)
                .withMemberCount(replicaCount)
                .withUseKeyfile(useInternalAuth)
                .withUseTls(useTls)
                .withOcp(ocp)
                .withProject(project)
                .build();
        replicaSet.start();
        synchronized (shardReplicaSets) {
            shardReplicaSets.add(replicaSet);
        }
        return replicaSet;
    }

    private void registerShardInMongos(@Nullable Map<MongoShardKey, ShardKeyRange> rangeMap, OcpMongoReplicaSet rs) {
        StringBuilder command = new StringBuilder();
        command.append(addShardAndZoneInMongosCommand(rs));

        if (rangeMap != null) {
            rangeMap.forEach((k, z) -> command.append(addShardKeyRangeCommand(k, z)));
        }
        executeMongoSh(command.toString());
    }

    private void deployConfigServers() {
        OcpMongoReplicaSet replicaSet = OcpMongoReplicaSet.builder()
                .withName(OcpMongoShardedConstants.MONGO_CONFIG_REPLICASET_NAME)
                .withConfigServer(true)
                .withRootUserName(rootUserName)
                .withRootPassword(rootPassword)
                .withMemberCount(configServerCount)
                .withUseKeyfile(useInternalAuth)
                .withUseTls(useTls)
                .withOcp(ocp)
                .withProject(project)
                .build();
        replicaSet.start();
        configServerReplicaSet = replicaSet;
    }

    private void deployMongos() {
        mongosRouter = new OcpMongoDeploymentManager(OcpMongosModelProvider.mongosDeployment(configServerReplicaSet.getReplicaSetFullName()),
                OcpMongosModelProvider.mongosService(), null, ocp, project);
        if (useInternalAuth) {
            MongoShardedUtil.addKeyFileToDeployment(mongosRouter.getDeployment());
        }

        if (useTls) {
            MongoShardedUtil.addCertificatesToDeployment(mongosRouter.getDeployment());
        }

        LOGGER.info("Deploying mongos");
        mongosRouter.start();
    }

    private void initMongos() throws IOException, TemplateException, InterruptedException {
        LOGGER.info("Initializing mongos...");
        Thread.sleep(5000);
        StringBuilder command = new StringBuilder();
        // create shards
        shardReplicaSets.forEach(rs -> command.append(addShardAndZoneInMongosCommand(rs)));

        // setup sharding keys and key ranges
        command.append("sh.enableSharding(\"" + DATABASE_MONGO_DBZ_DBNAME + "\");\n");
        shardKeys.forEach(collection -> command.append(this.shardCollectionCommand(collection)));
        shardKeys.forEach(k -> {
            k.getKeyRanges().forEach(z -> command.append(createKeyRangeCommand(z, k)));
        });

        executeMongoSh(command.toString());
    }

    private String addShardKeyRangeCommand(MongoShardKey key, ShardKeyRange range) {
        var keyMatch = this.shardKeys.stream().filter(k -> k.equals(key)).findFirst();
        if (keyMatch.isEmpty()) {
            throw new IllegalArgumentException("Illegal shard key");
        }
        keyMatch.get().getKeyRanges().add(range);
        return createKeyRangeCommand(range, key);
    }

    private String addShardAndZoneInMongosCommand(OcpMongoReplicaSet shardRs) {
        return "sh.addShard(\"" + shardRs.getReplicaSetFullName() + "\");\n " +
                "sh.addShardToZone(\"" + shardRs.getName() + "\", \"" + shardRs.getName() + "\");\n";
    }

    private String shardCollectionCommand(MongoShardKey key) {
        return String.format("sh.shardCollection(\"%s\", { _id: %s } );\n", key.getCollection(), key.getShardingType().getValue());
    }

    private String createKeyRangeCommand(ShardKeyRange range, MongoShardKey key) {
        return String.format("sh.updateZoneKeyRange(\"%s\",{ %s : %s },{ %s : %s },\"%s\");\n", key.getCollection(), key.getKey(), range.getStart(), key.getKey(),
                range.getEnd(), range.getShardName());
    }

    public OcpMongoShardedCluster(int initialShardCount, int replicaCount, int configServerCount, @Nullable String rootUserName, @Nullable String rootPassword,
                                  boolean useInternalAuth, boolean useTls, OpenShiftClient ocp, String project, List<MongoShardKey> shardKeys) {
        this.initialShardCount = initialShardCount;
        this.replicaCount = replicaCount;
        this.configServerCount = configServerCount;
        this.rootUserName = StringUtils.isNotEmpty(rootUserName) ? rootUserName : ConfigProperties.DATABASE_MONGO_USERNAME;
        this.rootPassword = StringUtils.isNotEmpty(rootPassword) ? rootPassword : ConfigProperties.DATABASE_MONGO_SA_PASSWORD;
        this.useInternalAuth = useInternalAuth;
        this.useTls = useTls;
        this.ocp = ocp;
        this.project = project;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.shardKeys = shardKeys;
    }

    public static OcpMongoShardedClusterBuilder builder() {
        return new OcpMongoShardedClusterBuilder();
    }

    public boolean getUseTls() {
        return useTls;
    }

    public static final class OcpMongoShardedClusterBuilder {
        private int replicaCount;
        private int configServerCount;
        private String rootUserName;
        private String rootPassword;
        private boolean useInternalAuth;
        private OpenShiftClient ocp;
        private int initialShardCount;
        private String project;
        private List<MongoShardKey> shardKeys;
        private boolean useTls;

        private OcpMongoShardedClusterBuilder() {
        }

        public OcpMongoShardedClusterBuilder withReplicaCount(int replicaCount) {
            this.replicaCount = replicaCount;
            return this;
        }

        public OcpMongoShardedClusterBuilder withConfigServerCount(int configServerCount) {
            this.configServerCount = configServerCount;
            return this;
        }

        public OcpMongoShardedClusterBuilder withRootUser(String rootUserName, String rootPassword) {
            this.rootUserName = rootUserName;
            this.rootPassword = rootPassword;
            return this;
        }

        public OcpMongoShardedClusterBuilder withUseInternalAuth(boolean useInternalAuth) {
            this.useInternalAuth = useInternalAuth;
            return this;
        }

        public OcpMongoShardedClusterBuilder withUseTls(boolean useTls) {
            this.useTls = useTls;
            return this;
        }

        public OcpMongoShardedClusterBuilder withOcp(OpenShiftClient ocp) {
            this.ocp = ocp;
            return this;
        }

        public OcpMongoShardedClusterBuilder withInitialShardCount(int initialShardCount) {
            this.initialShardCount = initialShardCount;
            return this;
        }

        public OcpMongoShardedClusterBuilder withProject(String project) {
            this.project = project;
            return this;
        }

        public OcpMongoShardedClusterBuilder withShardKeys(List<MongoShardKey> shardKeys) {
            this.shardKeys = shardKeys;
            return this;
        }

        public OcpMongoShardedCluster build() {
            return new OcpMongoShardedCluster(initialShardCount, replicaCount, configServerCount, rootUserName, rootPassword, useInternalAuth, useTls, ocp, project,
                    shardKeys);
        }
    }
}

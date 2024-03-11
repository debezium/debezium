/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbContainer.router;
import static io.debezium.testing.testcontainers.MongoDbReplicaSet.configServerReplicaSet;
import static io.debezium.testing.testcontainers.MongoDbReplicaSet.replicaSet;
import static io.debezium.testing.testcontainers.util.DockerUtils.logDockerDesktopBanner;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.MongoDbContainer.Address;
import io.debezium.testing.testcontainers.util.MoreStartables;
import io.debezium.testing.testcontainers.util.PortResolver;
import io.debezium.testing.testcontainers.util.RandomPortResolver;

/**
 * A MongoDB sharded cluster.
 */
public class MongoDbShardedCluster implements MongoDbDeployment {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbShardedCluster.class);

    private final int shardCount;
    private final int replicaCount;
    private final int routerCount;
    private final Network network;
    private final PortResolver portResolver;

    private final MongoDbReplicaSet configServers;
    private final List<MongoDbReplicaSet> shards;
    private final List<MongoDbContainer> routers;
    private final DockerImageName imageName;

    private volatile boolean started;

    public static Builder shardedCluster() {
        return new Builder();
    }

    public static class Builder {

        private static final Network commonNetwork = Network.newNetwork();

        private int shardCount = 1;
        private int replicaCount = 1;
        private int routerCount = 1;

        private Network network = commonNetwork;
        private PortResolver portResolver = new RandomPortResolver();
        private boolean skipDockerDesktopLogWarning = false;
        private DockerImageName imageName;

        public Builder imageName(DockerImageName imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder shardCount(int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder replicaCount(int replicaCount) {
            this.replicaCount = replicaCount;
            return this;
        }

        public Builder routerCount(int routerCount) {
            this.routerCount = routerCount;
            return this;
        }

        public Builder network(Network network) {
            this.network = network;
            return this;
        }

        public Builder portResolver(PortResolver portResolver) {
            this.portResolver = portResolver;
            return this;
        }

        public Builder skipDockerDesktopLogWarning(boolean skipDockerDesktopLogWarning) {
            this.skipDockerDesktopLogWarning = skipDockerDesktopLogWarning;
            return this;
        }

        public MongoDbShardedCluster build() {
            return new MongoDbShardedCluster(this);
        }
    }

    private MongoDbShardedCluster(Builder builder) {
        this.shardCount = builder.shardCount;
        this.replicaCount = builder.replicaCount;
        this.routerCount = builder.routerCount;
        this.network = builder.network;
        this.portResolver = builder.portResolver;
        this.imageName = builder.imageName;

        this.shards = createShards();
        this.configServers = createConfigServers();
        this.routers = createRouters();

        logDockerDesktopBanner(LOGGER, getHostNames(), builder.skipDockerDesktopLogWarning);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        // `start` needs to be reentrant for `Startables.deepStart` or it will be sad
        if (started) {
            return;
        }

        LOGGER.info("Starting {} shard cluster...", shards.size());
        MoreStartables.deepStartSync(stream());

        addShards();

        started = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (started) {
            LOGGER.info("Stopping {} shard cluster...", shards.size());
            MoreStartables.deepStopSync(stream());
            started = false;
        }
    }

    public int size() {
        return shards.size();
    }

    /**
     * @return the <a href="https://www.mongodb.com/docs/manual/reference/connection-string/#standard-connection-string-format">standard connection string</a>
     * to the sharded cluster, comprised of only the {@code mongos} hosts.
     */
    public String getConnectionString() {
        return "mongodb://" + routers.stream()
                .map(MongoDbContainer::getClientAddress)
                .map(Objects::toString)
                .collect(joining(","));
    }

    public MongoDbReplicaSet getShard(int i) {
        return shards.get(i);
    }

    public void enableSharding(String databaseName) {
        // Note: No longer required in 6.0
        // See https://www.mongodb.com/docs/v5.0/reference/method/sh.enableSharding/
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval("sh.enableSharding('" + databaseName + "')");
    }

    public void shardCollection(String databaseName, String collectionName, String keyField) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#shard-a-collection
        LOGGER.info("Enabling sharding for {}.{} using '{}' filed as key", databaseName, collectionName, keyField);
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval("sh.shardCollection(" +
                "'" + databaseName + "." + collectionName + "'," +
                "{" + keyField + ":'hashed'}" + // Note: only hashing supported for testing
                ")");
    }

    private List<MongoDbReplicaSet> createShards() {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#create-the-shard-replica-sets
        return rangeClosed(1, shardCount)
                .mapToObj(this::createShard)
                .collect(toList());
    }

    private MongoDbReplicaSet createShard(int i) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#start-each-member-of-the-shard-replica-set
        var shard = MongoDbReplicaSet.shardReplicaSet()
                .network(network)
                .namespace("test-mongo-shard" + i + "-replica")
                .name("shard" + i)
                .memberCount(replicaCount)
                .portResolver(portResolver)
                .skipDockerDesktopLogWarning(true)
                .imageName(imageName)
                .build();

        return shard;
    }

    private MongoDbReplicaSet createConfigServers() {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#create-the-config-server-replica-set
        var configServers = configServerReplicaSet()
                .network(network)
                .namespace("test-mongo-configdb")
                .name("configdb")
                .memberCount(replicaCount)
                .portResolver(portResolver)
                .configServer(true)
                .skipDockerDesktopLogWarning(true)
                .imageName(imageName)
                .build();

        return configServers;
    }

    private List<MongoDbContainer> createRouters() {
        return rangeClosed(1, routerCount)
                .mapToObj(i -> createRouter(network, i))
                .collect(toList());
    }

    private MongoDbContainer createRouter(Network network, int i) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#start-a-mongos-for-the-sharded-cluster
        var router = router(formatReplicaSetAddress(configServers, /* namedAddess= */ true))
                .network(network)
                .name("test-mongos" + i)
                .portResolver(portResolver)
                .skipDockerDesktopLogWarning(true)
                .imageName(imageName)
                .build();

        router.getDependencies().addAll(shards);
        router.getDependencies().add(configServers);

        return router;
    }

    /**
     * Invokes <a hrer="https://www.mongodb.com/docs/v6.0/reference/method/sh.addShard/#mongodb-method-sh.addShard">sh.addShard</a>
     * on a router to add a new shard replica set to the sharded cluster.
     */
    public void addShard() {
        // Create shard replica set
        var shard = createShard(shards.size() + 1);
        shard.start();

        // Add to shard
        addShard(shard);

        // Make visible to clients
        shards.add(shard);
    }

    /**
     * Invokes the <a href="https://www.mongodb.com/docs/manual/reference/command/removeShard/">removeShard</a> command to
     * remove the last added {@code shard} from the sharded cluster.
     * <p>
     * Waits until the state of the shard is {@code completed} before shutting down the shard replica set.
     */
    public void removeShard() {
        var shard = shards.remove(shards.size() - 1);
        LOGGER.info("Removing shard: {}", shard.getName());

        var arbitraryRouter = routers.get(0);
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> arbitraryRouter.eval("db.adminCommand({removeShard: '" + shard.getName() + "'})")
                        .path("state")
                        .asText()
                        .equals("completed"));

        shard.stop();
    }

    /**
     * Adds the previously created and started shards to the shards to the cluster.
     */
    private void addShards() {
        shards.forEach(this::addShard);
    }

    /**
     * Adds the previously created and started supplied {@code shard} to the cluster.
     *
     * @param shard the shard to add
     */
    private void addShard(MongoDbReplicaSet shard) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#add-shards-to-the-cluster
        var shardAddress = formatReplicaSetAddress(shard, /* namedAddess= */ false);
        LOGGER.info("Adding shard: {}", shardAddress);

        // https://www.mongodb.com/docs/manual/reference/command/addShard/
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval("sh.addShard('" + shardAddress + "')");

        // https://www.mongodb.com/docs/manual/reference/command/listShards/
        await()
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> stream(arbitraryRouter.eval("db.adminCommand({listShards: 1})")
                        .path("shards"))
                        .anyMatch(s -> s.get("_id").asText().equals(shard.getName()) &&
                                s.get("state").asInt() == 1));
    }

    private Stream<Startable> stream() {
        return Stream.concat(Stream.concat(shards.stream(), Stream.of(configServers)), routers.stream());
    }

    public List<String> getHostNames() {
        var hostsEntries = new ArrayList<String>();

        routers.stream()
                .map(MongoDbContainer::getNamedAddress)
                .map(Address::getHost)
                .forEach(hostsEntries::add);

        shards.stream()
                .map(MongoDbReplicaSet::getHostNames)
                .forEach(hostsEntries::addAll);

        configServers.getHostNames()
                .forEach(hostsEntries::add);

        return hostsEntries;
    }

    private static String formatReplicaSetAddress(MongoDbReplicaSet replicaSet, boolean namedAddress) {
        // See https://www.mongodb.com/docs/v6.0/reference/method/sh.addShard/#mongodb-method-sh.addShard
        return replicaSet.getName() + "/" + replicaSet.getMembers().stream()
                .map(namedAddress ? MongoDbContainer::getNamedAddress : MongoDbContainer::getClientAddress)
                .map(Object::toString)
                .collect(joining(","));
    }

    @Override
    public String toString() {
        return "MongoDbShardedCluster{" +
                "configServers=" + configServers +
                ", shards=" + shards +
                ", routers=" + routers +
                ", started=" + started +
                '}';
    }

    private static <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.cluster;

import static io.debezium.connector.mongodb.cluster.MongoDbNode.node;
import static io.debezium.connector.mongodb.cluster.MongoDbReplicaSet.replicaSet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

/**
 * A MongoDB sharded cluster.
 */
public class MongoDbShardedCluster implements Startable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbShardedCluster.class);

    private final int shardCount;
    private final int replicaCount;
    private final int routerCount;
    private final Network network;

    private final MongoDbReplicaSet configServers;
    private final List<MongoDbReplicaSet> shards;
    private final List<MongoDbNode> routers;

    private volatile boolean started;

    public static Builder shardedCluster() {
        return new Builder();
    }

    public static class Builder {

        private int shardCount = 1;
        private int replicaCount = 1;
        private int routerCount = 1;

        private Network network = Network.newNetwork();

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

        public MongoDbShardedCluster build() {
            return new MongoDbShardedCluster(this);
        }
    }

    private MongoDbShardedCluster(Builder builder) {
        this.shardCount = builder.shardCount;
        this.replicaCount = builder.replicaCount;
        this.routerCount = builder.routerCount;
        this.network = builder.network;

        this.shards = createShards();
        this.configServers = createConfigServers();
        this.routers = createRouters();
    }

    @Override
    public void start() {
        // `start` needs to be reentrant for `Startables.deepStart` or it will be sad
        if (started) {
            return;
        }
        try {
            Startables.deepStart(stream().toArray(Startable[]::new)).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }

        addShards();

        started = true;
    }

    @Override
    public void stop() {
        // Idempotent
        try {
            allOf(stream().map(node -> runAsync(node::stop)).toArray(CompletableFuture[]::new)).get();
            network.close();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return the <a href="https://www.mongodb.com/docs/manual/reference/connection-string/#standard-connection-string-format">standard connection string</a>
     * to the sharded cluster, comprised of only the {@code mongos} hosts.
     */
    public String getConnectionString() {
        return "mongodb://" + routers.stream()
                .map(MongoDbNode::getAddress)
                .map(Objects::toString)
                .collect(joining(","));
    }

    public void enableSharding(String databaseName) {
        // Note: No longer required in 6.0
        // See https://www.mongodb.com/docs/v5.0/reference/method/sh.enableSharding/
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval("sh.enableSharding('" + databaseName + "');");
    }

    public void shardCollection(String databaseName, String collectionName, String keyField) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#shard-a-collection
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval("sh.shardCollection(" +
                "'" + databaseName + "." + collectionName + "'," +
                "{" + keyField + ":'hashed'}" + // Note: only hashing supported for testing
                ");");
    }

    private List<MongoDbReplicaSet> createShards() {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#create-the-shard-replica-sets
        return rangeClosed(1, shardCount)
                .mapToObj(this::createShard)
                .collect(toList());
    }

    private MongoDbReplicaSet createShard(int i) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#start-each-member-of-the-shard-replica-set
        var shard = replicaSet()
                .network(network)
                .namespace("test-mongo-shard" + i + "-replica")
                .name("shard" + i).memberCount(replicaCount)
                .build();

        shard.getMembers().forEach(node -> node.setCommand(
                "--shardsvr",
                "--replSet", shard.getName(),
                "--port", String.valueOf(node.getAddress().getPort()),
                "--bind_ip", "localhost," + node.getAddress().getHost()));

        return shard;
    }

    private MongoDbReplicaSet createConfigServers() {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#create-the-config-server-replica-set
        var configServers = replicaSet()
                .network(network)
                .namespace("test-mongo-configdb")
                .name("configdb")
                .memberCount(replicaCount)
                .configServer(true)
                .build();

        configServers.getMembers().forEach(node -> node.setCommand(
                "--configsvr",
                "--replSet", configServers.getName(),
                "--port", String.valueOf(node.getAddress().getPort()),
                "--bind_ip", "localhost," + node.getAddress().getHost()));

        return configServers;
    }

    private List<MongoDbNode> createRouters() {
        return rangeClosed(1, routerCount)
                .mapToObj(i -> createRouter(network, i))
                .collect(toList());
    }

    private MongoDbNode createRouter(Network network, int i) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#start-a-mongos-for-the-sharded-cluster
        var router = node()
                .network(network)
                .name("test-mongos" + i)
                .build();

        router.setCommand(
                "mongos",
                "--port", String.valueOf(router.getAddress().getPort()),
                "--bind_ip", "localhost," + router.getAddress().getHost(),
                "--configdb", formatReplicaSetAddress(configServers));
        router.getDependencies().addAll(shards);
        router.getDependencies().add(configServers);

        return router;
    }

    public void addShard() {
        var shard = createShard(shards.size() + 1);
        shard.start();
        shards.add(shard);
        addShard(shard);
    }

    public void removeShard() {
        var shard = shards.remove(shards.size() - 1);
        removeShard(shard);
        shard.stop();
    }

    private void addShards() {
        shards.forEach(this::addShard);
    }

    private void addShard(MongoDbReplicaSet shard) {
        // See https://www.mongodb.com/docs/v6.0/tutorial/deploy-shard-cluster/#add-shards-to-the-cluster
        var shardAddress = formatReplicaSetAddress(shard);
        LOGGER.info("Adding shard: {}", shardAddress);
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval(
                "sh.addShard('" + shardAddress + "');");
    }

    private void removeShard(MongoDbReplicaSet shard) {
        // See https://www.mongodb.com/docs/manual/reference/command/removeShard/
        LOGGER.info("Removing shard: {}", shard.getName());
        var arbitraryRouter = routers.get(0);
        arbitraryRouter.eval(
                "db.adminCommand({removeShard: '" + shard.getName() + "'});");
    }

    private Stream<Startable> stream() {
        return Stream.concat(Stream.concat(shards.stream(), Stream.of(configServers)), routers.stream());
    }

    private static String formatReplicaSetAddress(MongoDbReplicaSet replicaSet) {
        // See https://www.mongodb.com/docs/v6.0/reference/method/sh.addShard/#mongodb-method-sh.addShard
        return replicaSet.getName() + "/" + replicaSet.getMembers().stream()
                .map(MongoDbNode::getAddress)
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

}

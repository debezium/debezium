/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import org.testcontainers.containers.Network;

import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.MongoDbShardedCluster;
import io.debezium.testing.testcontainers.util.ParsingPortResolver;

public final class NetworkIsolatedMongoDbDatabaseProvider {

    public static final String MONGO_REPLICA_SIZE = "mongodb.replica.size";
    public static final String MONGO_SHARD_SIZE = "mongodb.shard.size";
    public static final String MONGO_SHARD_REPLICA_SIZE = "mongodb.shard.replica.size";
    public static final String MONGO_DOCKER_DESKTOP_PORT_PROPERTY = "mongodb.docker.desktop.ports";

    // Should be aligned with definition in pom.xml
    public static final String MONGO_DOCKER_DESKTOP_PORT_DEFAULT = "27017:27117";

    private final Network network;

    public NetworkIsolatedMongoDbDatabaseProvider(Network network) {
        this.network = network;
    }

    public NetworkIsolatedMongoDbDatabaseProvider() {
        this(null);
    }

    private MongoDbReplicaSet.Builder dockerReplicaSetBuilder() {
        // will be used only in environment with docker desktop
        var portResolver = ParsingPortResolver.parseProperty(MONGO_DOCKER_DESKTOP_PORT_PROPERTY, MONGO_DOCKER_DESKTOP_PORT_DEFAULT);
        var replicaSize = Integer.parseInt(System.getProperty(MONGO_REPLICA_SIZE, "1"));

        var builder = MongoDbReplicaSet.replicaSet()
                .memberCount(replicaSize)
                .portResolver(portResolver);
        if (network != null) {
            builder.network(network);
        }
        return builder;
    }

    /**
     * Constructs the default testing MongoDB replica set
     *
     * @return MongoDb Replica set
     */
    public MongoDbReplicaSet dockerReplicaSet() {
        return dockerReplicaSetBuilder().build();
    }

    /**
     * Constructs testing MongoDB replica set with enabled authentication
     *
     * @return MongoDb Replica set
     */
    public MongoDbReplicaSet dockerAuthReplicaSet() {
        return dockerReplicaSetBuilder().authEnabled(true).build();
    }

    /**
     * Constructs the default testing MongoDB sharded cluster
     *
     * @return MongoDb Replica set
     */
    private MongoDbShardedCluster.Builder mongoDbShardedClusterBuilder() {
        // will be used only in environment with docker desktop
        var portResolver = ParsingPortResolver.parseProperty(MONGO_DOCKER_DESKTOP_PORT_PROPERTY, MONGO_DOCKER_DESKTOP_PORT_DEFAULT);
        var shardSize = Integer.parseInt(System.getProperty(MONGO_SHARD_SIZE, "2"));
        var replicaSize = Integer.parseInt(System.getProperty(MONGO_SHARD_REPLICA_SIZE, "1"));

        var builder = MongoDbShardedCluster.shardedCluster()
                .shardCount(shardSize)
                .replicaCount(replicaSize)
                .routerCount(1)
                .portResolver(portResolver);

        if (network != null) {
            builder.network(network);
        }

        return builder;
    }

    public MongoDbShardedCluster mongoDbShardedCluster() {
        return mongoDbShardedClusterBuilder().build();
    }
}

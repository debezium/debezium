/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import io.debezium.testing.testcontainers.MongoDbReplicaSet;
import io.debezium.testing.testcontainers.MongoDbShardedCluster;
import io.debezium.testing.testcontainers.util.ParsingPortResolver;

public final class MongoDbDatabaseProvider {

    public static final String MONGO_REPLICA_SIZE = "mongodb.replica.size";
    public static final String MONGO_SHARD_SIZE = "mongodb.shard.size";
    public static final String MONGO_SHARD_REPLICA_SIZE = "mongodb.shard.replica.size";
    public static final String MONGO_DOCKER_DESKTOP_PORT_PROPERTY = "mongodb.docker.desktop.ports";

    // Should be aligned with definition in pom.xm
    public static final String MONGO_DOCKER_DESKTOP_PORT_DEFAULT = "27017:27117";

    /**
     * Constructs the default testing MongoDB replica set
     *
     * @return MongoDb Replica set
     */
    public static MongoDbReplicaSet mongoDbReplicaSet() {
        // will be used only in environment with docker desktop
        var portResolver = ParsingPortResolver.parseProperty(MONGO_DOCKER_DESKTOP_PORT_PROPERTY, MONGO_DOCKER_DESKTOP_PORT_DEFAULT);
        var replicaSize = Integer.parseInt(System.getProperty(MONGO_REPLICA_SIZE, "1"));

        return MongoDbReplicaSet.replicaSet()
                .memberCount(replicaSize)
                .portResolver(portResolver)
                .build();
    }

    /**
     * Constructs the default testing MongoDB sharded cluster
     *
     * @return MongoDb Replica set
     */
    public static MongoDbShardedCluster mongoDbShardedCluster() {
        // will be used only in environment with docker desktop
        var portResolver = ParsingPortResolver.parseProperty(MONGO_DOCKER_DESKTOP_PORT_PROPERTY, MONGO_DOCKER_DESKTOP_PORT_DEFAULT);
        var shardSize = Integer.parseInt(System.getProperty(MONGO_SHARD_SIZE, "2"));
        var replicaSize = Integer.parseInt(System.getProperty(MONGO_SHARD_REPLICA_SIZE, "1"));

        return MongoDbShardedCluster.shardedCluster()
                .shardCount(shardSize)
                .replicaCount(replicaSize)
                .routerCount(1)
                .portResolver(portResolver)
                .build();
    }

    private MongoDbDatabaseProvider() {
    }
}

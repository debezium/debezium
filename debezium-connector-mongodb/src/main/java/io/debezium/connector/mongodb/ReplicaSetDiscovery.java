/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.ReplicaSet;

/**
 * A component that monitors a single replica set or the set of replica sets that make up the shards in a sharded cluster.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class ReplicaSetDiscovery {
    /**
     * The database that might be used to check for member information in a replica set.
     */
    public static final String ADMIN_DATABASE_NAME = "admin";

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSetDiscovery.class);

    private final MongoDbTaskContext context;
    private final String maskedConnectionSeed;

    /**
     * Create a cluster component.
     *
     * @param context the replication context; may not be null
     */
    public ReplicaSetDiscovery(MongoDbTaskContext context) {
        this.context = context;
        this.maskedConnectionSeed = context.getConnectionContext().maskedConnectionSeed();
    }

    /**
     * Connect to the shard cluster or replica set defined by the seed addresses, and obtain the specifications for each of the
     * replica sets.
     *
     * @return the information about the replica sets; never null but possibly empty
     */
    public ReplicaSets getReplicaSets(MongoClient client) {
        ConnectionContext connectionContext = context.getConnectionContext();
        Set<ReplicaSet> replicaSetSpecs = new HashSet<>();

        LOGGER.info("Reading description of cluster at {}", maskedConnectionSeed);
        final ClusterDescription clusterDescription = MongoUtil.clusterDescription(client);

        if (clusterDescription.getType() == ClusterType.SHARDED) {
            LOGGER.info("Cluster identified as sharded cluster");
            readShardedClusterAsReplicaSet(replicaSetSpecs, connectionContext);
        }

        if (clusterDescription.getType() == ClusterType.REPLICA_SET) {
            LOGGER.info("Cluster identified as replicaSet");
            readReplicaSetsFromCluster(replicaSetSpecs, clusterDescription, connectionContext);
        }

        if (replicaSetSpecs.isEmpty()) {
            LOGGER.error("Found no replica sets at {}, so there is nothing to monitor and no connector tasks will be started.", maskedConnectionSeed);
        }

        return new ReplicaSets(replicaSetSpecs);
    }

    private void readShardedClusterAsReplicaSet(Set<ReplicaSet> replicaSetSpecs, ConnectionContext connectionContext) {
        LOGGER.info("Using '{}' as sharded cluster connection", maskedConnectionSeed);
        var connectionString = connectionContext.connectionString();
        replicaSetSpecs.add(new ReplicaSet(connectionString));
    }

    private void readReplicaSetsFromCluster(Set<ReplicaSet> replicaSetSpecs, ClusterDescription clusterDescription, ConnectionContext connectionContext) {
        var connectionString = ensureReplicaSetName(connectionContext.connectionSeed(), clusterDescription);

        LOGGER.info("Using '{}' as replica set connection string", ConnectionStrings.mask(connectionString));
        replicaSetSpecs.add(new ReplicaSet(connectionString));
    }

    /**
     * Ensures connection string contains the replicaSet parameter.If connection string doesn't contain the parameter,
     * the replica set name is read from cluster description and the parameter is added.
     *
     * @param connectionString the original connection string
     * @param clusterDescription cluster description
     * @return connection string with replicaSet parameter
     */
    private String ensureReplicaSetName(String connectionString, ClusterDescription clusterDescription) {
        // If we have replicaSet parameter then just return
        var cs = new ConnectionString(connectionString);
        if (cs.getRequiredReplicaSetName() != null) {
            return connectionString;
        }

        // Otherwise Java driver is smart enough to connect correctly
        // However replicaSet parameter is mandatory, and we need the name for offset storage
        LOGGER.warn("Replica set not specified in '{}'", maskedConnectionSeed);
        LOGGER.warn("Parameter 'replicaSet' should be added to connection string");
        LOGGER.warn("Trying to determine replica set name for '{}'", maskedConnectionSeed);
        var rsName = MongoUtil.replicaSetName(clusterDescription);

        if (rsName.isPresent()) {
            LOGGER.info("Found '{}' replica set for '{}'", rsName.get(), maskedConnectionSeed);
            return ConnectionStrings.appendParameter(connectionString, "replicaSet", rsName.get());
        }

        LOGGER.warn("Unable to find replica set name for '{}'", maskedConnectionSeed);
        return connectionString;
    }
}

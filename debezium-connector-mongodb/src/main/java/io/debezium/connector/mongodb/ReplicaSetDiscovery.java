/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
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
    public ReplicaSet getReplicaSet() {
        ConnectionContext connectionContext = context.getConnectionContext();

        try (var client = connectionContext.connect()) {
            LOGGER.info("Reading description of cluster at {}", maskedConnectionSeed);
            final ClusterDescription clusterDescription = MongoUtil.clusterDescription(client);

            if (clusterDescription.getType() == ClusterType.SHARDED) {
                LOGGER.info("Cluster identified as sharded cluster");
                return readShardedClusterAsReplicaSet(connectionContext);
            }

            if (clusterDescription.getType() == ClusterType.REPLICA_SET) {
                LOGGER.info("Cluster identified as replicaSet");
                return readReplicaSet(clusterDescription, connectionContext);
            }
        }

        return null;
    }

    private ReplicaSet readShardedClusterAsReplicaSet(ConnectionContext connectionContext) {
        LOGGER.info("Using '{}' as sharded cluster connection", maskedConnectionSeed);
        var connectionString = connectionContext.connectionString();
        return new ReplicaSet(connectionString);
    }

    private ReplicaSet readReplicaSet(ClusterDescription clusterDescription, ConnectionContext connectionContext) {
        var connectionString = ensureReplicaSetName(connectionContext.connectionSeed(), clusterDescription);

        LOGGER.info("Using '{}' as replica set connection string", ConnectionStrings.mask(connectionString));
        return new ReplicaSet(connectionString);
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

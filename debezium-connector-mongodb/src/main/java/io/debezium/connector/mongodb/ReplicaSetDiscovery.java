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

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.annotation.ThreadSafe;

/**
 * A component that monitors a single replica set or the set of replica sets that make up the shards in a sharded cluster.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class ReplicaSetDiscovery {

    /**
     * The database that might be used to check for replica set information in a sharded cluster.
     */
    public static final String CONFIG_DATABASE_NAME = "config";

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
    public ReplicaSets getReplicaSets() {
        ConnectionContext connectionContext = context.getConnectionContext();
        MongoClient client = connectionContext.connect();
        Set<ReplicaSet> replicaSetSpecs = new HashSet<>();

        final ClusterDescription clusterDescription = MongoUtil.clusterDescription(client);

        if (clusterDescription.getType() == ClusterType.SHARDED) {
            LOGGER.info("Cluster at {} identified as sharded cluster", maskedConnectionSeed);

            // Gather connection details to each shard ...
            String shardsCollection = "shards";
            try {
                MongoUtil.onCollectionDocuments(client, CONFIG_DATABASE_NAME, shardsCollection, doc -> {
                    String shardName = doc.getString("_id");
                    String hostStr = doc.getString("host");

                    LOGGER.info("Reading shard details for {}", shardName);

                    ConnectionStrings.parseFromHosts(hostStr).ifPresentOrElse(
                            cs -> replicaSetSpecs.add(new ReplicaSet(cs)),
                            () -> LOGGER.info("Shard {} is not a valid replica set", shardName));
                });
            }
            catch (MongoInterruptedException e) {
                LOGGER.error("Interrupted while reading the '{}' collection in the '{}' database: {}",
                        shardsCollection, CONFIG_DATABASE_NAME, e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
            catch (MongoException e) {
                LOGGER.error("Error while reading the '{}' collection in the '{}' database: {}",
                        shardsCollection, CONFIG_DATABASE_NAME, e.getMessage(), e);
            }
        }

        if (clusterDescription.getType() == ClusterType.REPLICA_SET) {
            LOGGER.info("Cluster at '{}' identified as replicaSet", maskedConnectionSeed);

            var connectionString = connectionContext.connectionSeed();
            var cs = connectionContext.connectionString();

            if (cs.getRequiredReplicaSetName() == null) {
                // Java driver is smart enough to connect correctly
                // However replicaSet parameter is mandatory, and we need the name for offset storage
                LOGGER.warn("Replica set not specified in '{}'", maskedConnectionSeed);
                LOGGER.warn("Parameter 'replicaSet' should be added to connection string");
                LOGGER.warn("Trying to determine replica set name for '{}'", maskedConnectionSeed);
                var rsName = MongoUtil.replicaSetName(clusterDescription);

                if (rsName.isPresent()) {
                    LOGGER.info("Found '{}' replica set for '{}'", rsName.get(), maskedConnectionSeed);
                    connectionString = ConnectionStrings.appendParameter(connectionString, "replicaSet", rsName.get());
                }
                else {
                    LOGGER.warn("Unable to find replica set name for '{}'", maskedConnectionSeed);
                }
            }

            LOGGER.info("Using '{}' as replica set connection string", ConnectionStrings.mask(connectionString));
            replicaSetSpecs.add(new ReplicaSet(connectionString));
        }

        if (replicaSetSpecs.isEmpty()) {
            // Without a replica sets, we can't do anything ...
            LOGGER.error(
                    "Found no replica sets at {}, so there is nothing to monitor and no connector tasks will be started. Check seed addresses in connector configuration.",
                    maskedConnectionSeed);
        }
        return new ReplicaSets(replicaSetSpecs);
    }
}

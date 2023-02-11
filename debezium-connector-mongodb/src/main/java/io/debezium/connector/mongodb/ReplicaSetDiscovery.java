/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;

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
        MongoClient client = connectionContext.clientForSeedConnection();
        Set<ReplicaSet> replicaSetSpecs = new HashSet<>();

        final ClusterDescription clusterDescription = MongoUtil.clusterDescription(client);

        if (clusterDescription.getType() == ClusterType.SHARDED) {
            // First see if the addresses are for a config server replica set ...
            String shardsCollection = "shards";
            try {
                MongoUtil.onCollectionDocuments(client, CONFIG_DATABASE_NAME, shardsCollection, doc -> {
                    LOGGER.info("Checking shard details from configuration replica set {}", maskedConnectionSeed);
                    String shardName = doc.getString("_id");
                    String hostStr = doc.getString("host");
                    String replicaSetName = MongoUtil.replicaSetUsedIn(hostStr);
                    replicaSetSpecs.add(new ReplicaSet(hostStr, replicaSetName, shardName));
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
            LOGGER.info("Checking current members of replica set at {}", maskedConnectionSeed);
            final List<ServerDescription> serverDescriptions = clusterDescription.getServerDescriptions().stream()
                    .filter(x -> x.getState() == ServerConnectionState.CONNECTED).collect(Collectors.toList());
            if (serverDescriptions.size() == 0) {
                LOGGER.warn("Server descriptions not available, got '{}'", serverDescriptions);
            }
            else {
                List<ServerAddress> addresses = serverDescriptions.stream().map(ServerDescription::getAddress).collect(Collectors.toList());
                String replicaSetName = serverDescriptions.get(0).getSetName();
                replicaSetSpecs.add(new ReplicaSet(addresses, replicaSetName, null));
            }
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

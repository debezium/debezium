/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoUtils;
import io.debezium.connector.mongodb.connection.client.DefaultMongoDbClientFactory;
import io.debezium.connector.mongodb.connection.client.MongoDbClientFactory;

/**
 * @author Randall Hauch
 *
 */
public class MongoDbConnectionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbConnectionContext.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbClientFactory clientFactory;

    /**
     * @param config the configuration
     */
    public MongoDbConnectionContext(Configuration config) {
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.clientFactory = new DefaultMongoDbClientFactory(config);
    }

    public MongoDbConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public ConnectionString getConnectionString() {
        return connectorConfig.getConnectionString();
    }

    /**
     * Same as {@link #getConnectionString()} but masks sensitive information
     *
     * @return masked connection string
     */
    public String getMaskedConnectionString() {
        return ConnectionStrings.mask(getConnectionString());
    }

    /**
     * Creates native {@link MongoClient} instance
     *
     * @return mongo client
     */
    public MongoClient getMongoClient() {
        return clientFactory.getMongoClient();
    }

    public ClusterDescription getClusterDescription() {
        try (var client = getMongoClient()) {
            LOGGER.info("Reading description of cluster at {}", getMaskedConnectionString());
            return MongoUtils.clusterDescription(client);
        }
    }

    public ClusterType getClusterType() {
        return getClusterDescription().getType();
    }

    public boolean isShardedCluster() {
        return ClusterType.SHARDED == getClusterType();
    }

    public Set<String> getShardNames() {
        if (!isShardedCluster()) {
            return Set.of();
        }

        var shardNames = new HashSet<String>();
        try (var client = getMongoClient()) {
            MongoUtils.onCollectionDocuments(client, "config", "shards", doc -> {
                String shardName = doc.getString("_id");
                shardNames.add(shardName);
            });
        }
        catch (Throwable t) {
            LOGGER.warn("Unable to read shard topology.");
        }
        return shardNames;
    }

    /**
     * @return Value specified by {@link ConnectionString#getRequiredReplicaSetName()} or empty optional
     */
    public Optional<String> getRequiredReplicaSetName() {
        return Optional.of(getConnectionString()).map(ConnectionString::getRequiredReplicaSetName);
    }

    /**
     * Determines if RS name is specified when required
     *
     * @return False if RS name is not specified, and we are connected to sharded cluster. True otherwise
     */
    public boolean hasRequiredReplicaSetName() {
        if (getRequiredReplicaSetName().isPresent()) {
            return true;
        }
        return getClusterDescription().getType() == ClusterType.SHARDED;
    }
}

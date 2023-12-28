/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoUtil;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbClientFactory clientFactory;

    /**
     * @param config the configuration
     */
    public ConnectionContext(Configuration config) {
        this.connectorConfig = new MongoDbConnectorConfig(config);

        // Set up the client pool so that it ...
        connectorConfig.getAuthProvider().init(config);
        clientFactory = MongoDbClientFactory.create(settings -> {
            settings.applyToSocketSettings(builder -> builder
                    .connectTimeout(connectorConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                    .readTimeout(connectorConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(
                            builder -> builder.serverSelectionTimeout(connectorConfig.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS))
                    .applyToServerSettings(
                            builder -> builder.heartbeatFrequency(connectorConfig.getHeartbeatFrequencyMs(), TimeUnit.MILLISECONDS));

            connectorConfig.getAuthProvider().addAuthConfig(settings);

            if (connectorConfig.isSslEnabled()) {
                settings.applyToSslSettings(
                        builder -> builder.enabled(true).invalidHostNameAllowed(connectorConfig.isSslAllowInvalidHostnames()));
            }

            settings.applyToSocketSettings(builder -> builder.connectTimeout(connectorConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                    .readTimeout(connectorConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(
                            builder -> builder.serverSelectionTimeout(connectorConfig.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS));
        });
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
        return clientFactory.client(getConnectionString());
    }

    public ClusterDescription getClusterDescription() {
        try (var client = getMongoClient()) {
            LOGGER.info("Reading description of cluster at {}", getMaskedConnectionString());
            return MongoUtil.clusterDescription(client);
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
            MongoUtil.onCollectionDocuments(client, "config", "shards", doc -> {
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

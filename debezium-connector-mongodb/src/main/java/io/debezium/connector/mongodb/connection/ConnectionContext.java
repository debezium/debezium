/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.DebeziumException;
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

        final MongoDbAuthProvider authProvider = config.getInstance(MongoDbConnectorConfig.AUTH_PROVIDER_CLASS, MongoDbAuthProvider.class);
        final boolean useSSL = config.getBoolean(MongoDbConnectorConfig.SSL_ENABLED);
        final boolean sslAllowInvalidHostnames = config.getBoolean(MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);

        final int connectTimeoutMs = config.getInteger(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS);
        final int heartbeatFrequencyMs = config.getInteger(MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS);
        final int socketTimeoutMs = config.getInteger(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS);
        final int serverSelectionTimeoutMs = config.getInteger(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS);

        authProvider.init(config);

        // Set up the client pool so that it ...
        clientFactory = MongoDbClientFactory.create(settings -> {
            settings.applyToSocketSettings(builder -> builder
                    .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(
                            builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToServerSettings(
                            builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS));

            authProvider.addAuthConfig(settings);

            if (useSSL) {
                settings.applyToSslSettings(
                        builder -> builder.enabled(true).invalidHostNameAllowed(sslAllowInvalidHostnames));
            }

            settings.applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(
                            builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS));
        });
    }

    public MongoDbClientFactory getClientFactory() {
        return clientFactory;
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

    public ConnectionString resolveTaskConnectionString() {
        var clusterDescription = getClusterDescription();

        if (clusterDescription.getType() == ClusterType.SHARDED) {
            LOGGER.info("Cluster identified as sharded cluster");
            return getConnectionString();
        }

        if (clusterDescription.getType() == ClusterType.REPLICA_SET) {
            LOGGER.info("Cluster identified as replicaSet");
            return MongoUtil.ensureReplicaSetName(getConnectionString(), clusterDescription);

        }
        throw new DebeziumException("Unable to determine cluster type");
    }
}

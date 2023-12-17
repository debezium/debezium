/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Filters;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoUtil;
import io.debezium.function.BlockingConsumer;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    private final Configuration config;
    private final MongoDbConnectorConfig connectorConfig;
    private final MongoDbClientFactory clientFactory;

    /**
     * @param config the configuration
     */
    public ConnectionContext(Configuration config) {
        this.config = config;
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
            settings.applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
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

    public MongoDbConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    /**
     * Initial connection string which is either a host specification or connection string
     *
     * @return hosts or connection string
     */
    public String connectionSeed() {
        return config.getString(MongoDbConnectorConfig.CONNECTION_STRING);
    }

    public ConnectionString connectionString() {
        return new ConnectionString(connectionSeed());
    }

    public ConnectionString resolveTaskConnectionString() {
        try (var client = connect()) {
            LOGGER.info("Reading description of cluster at {}", maskedConnectionSeed());
            final ClusterDescription clusterDescription = MongoUtil.clusterDescription(client);

            if (clusterDescription.getType() == ClusterType.SHARDED) {
                LOGGER.info("Cluster identified as sharded cluster");
                return connectionString();
            }

            if (clusterDescription.getType() == ClusterType.REPLICA_SET) {
                LOGGER.info("Cluster identified as replicaSet");
                var connectionString = MongoUtil.ensureReplicaSetName(connectionSeed(), clusterDescription);
                return new ConnectionString(connectionString);
            }
        }

        throw new DebeziumException("Unable to determine cluster type");
    }

    /**
     * Same as {@link #connectionSeed()} but masks sensitive information
     *
     * @return masked connection seed
     */
    public String maskedConnectionSeed() {
        return ConnectionStrings.mask(connectionSeed());
    }

    public Duration pollInterval() {
        return Duration.ofMillis(config.getLong(MongoDbConnectorConfig.MONGODB_POLL_INTERVAL_MS));
    }

    public MongoClient connect() {
        return clientFactory.client(connectionString());
    }

    /**
     * Obtain a client scoped to specific replica set.
     *
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the node is unable to
     *            {@link MongoDbConnection#execute(String, BlockingConsumer)}  execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public MongoDbConnection connect(ConnectionString connectionString, Filters filters, MongoDbConnection.ErrorHandler errorHandler) {
        return new MongoDbConnection(connectionString, clientFactory, connectorConfig, filters, errorHandler);
    }

    public MongoDbConnection connect(Filters filters, MongoDbConnection.ErrorHandler errorHandler) {
        return connect(connectionString(), filters, errorHandler);
    }
}

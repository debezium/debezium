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
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Filters;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
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

        final String username = config.getString(MongoDbConnectorConfig.USER);
        final String password = config.getString(MongoDbConnectorConfig.PASSWORD);
        final String adminDbName = config.getString(MongoDbConnectorConfig.AUTH_SOURCE);
        final boolean useSSL = config.getBoolean(MongoDbConnectorConfig.SSL_ENABLED);
        final boolean sslAllowInvalidHostnames = config.getBoolean(MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);

        final int connectTimeoutMs = config.getInteger(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS);
        final int heartbeatFrequencyMs = config.getInteger(MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS);
        final int socketTimeoutMs = config.getInteger(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS);
        final int serverSelectionTimeoutMs = config.getInteger(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS);

        // Set up the client pool so that it ...
        clientFactory = MongoDbClientFactory.create(settings -> {
            settings.applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                    .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(
                            builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToServerSettings(
                            builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS));

            // Use credential if provided as properties
            if (username != null || password != null) {
                settings.credential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
            }
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
        String seed = config.getString(MongoDbConnectorConfig.CONNECTION_STRING);
        if (seed == null) {
            String hosts = config.getString(MongoDbConnectorConfig.HOSTS);
            seed = ConnectionStrings.buildFromHosts(hosts);
        }
        return seed;
    }

    public ConnectionString connectionString() {
        return new ConnectionString(connectionSeed());
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
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the node is unable to
     *            {@link MongoDbConnection#execute(String, BlockingConsumer)}  execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public MongoDbConnection connect(ReplicaSet replicaSet, Filters filters,
                                     MongoDbConnection.ErrorHandler errorHandler) {
        return new MongoDbConnection(replicaSet, clientFactory, connectorConfig, filters, errorHandler);
    }
}

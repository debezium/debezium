/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;
import io.debezium.function.BlockingConsumer;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionContext implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    protected final Configuration config;
    protected final MongoClients pool;

    /**
     * @param config the configuration
     */
    public ConnectionContext(Configuration config) {
        this.config = config;

        final ConnectionString connectionString = connectionString();

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
        MongoClients.Builder poolBuilder = MongoClients.create();

        poolBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToServerSettings(
                        builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS));

        // Use credential if provided as properties
        if (username != null || password != null) {
            poolBuilder.withCredential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
        }
        if (useSSL) {
            poolBuilder.settings().applyToSslSettings(
                    builder -> builder.enabled(true).invalidHostNameAllowed(sslAllowInvalidHostnames));
        }

        poolBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS));

        pool = poolBuilder.build();
    }

    protected Logger logger() {
        return LOGGER;
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
        return pool.client(connectionString());
    }

    /**
     * Obtain a client scoped to specific replica set.
     *
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the node is unable to
     *            {@link RetryingMongoClient#execute(String, BlockingConsumer)}  execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public RetryingMongoClient connect(ReplicaSet replicaSet, ReadPreference preference, Filters filters,
                                       BiConsumer<String, Throwable> errorHandler) {
        return new RetryingMongoClient(replicaSet, preference, pool::client, filters, errorHandler);
    }

    @Override
    public final void close() {
        try {
            // Closing all connections ...
            logger().info("Closing all connections to {}", maskedConnectionSeed());
            pool.clear();
        }
        catch (Throwable e) {
            logger().error("Unexpected error shutting down the MongoDB clients", e);
        }
    }
}

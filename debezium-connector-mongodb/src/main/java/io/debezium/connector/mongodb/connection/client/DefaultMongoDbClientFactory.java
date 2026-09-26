/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import java.util.concurrent.TimeUnit;

import org.bson.UuidRepresentation;

import com.mongodb.MongoClientSettings;
import com.mongodb.event.ClusterListener;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.connection.MongoDbAuthProvider;

public class DefaultMongoDbClientFactory implements MongoDbClientFactory {

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoClientSettings clientSettings;
    private final MongoDbAuthProvider authProvider;
    private volatile boolean closed;
    private int activeClients;

    public DefaultMongoDbClientFactory(Configuration config) {
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.authProvider = connectorConfig.getAuthProvider();
        try {
            this.authProvider.init(config);
            this.clientSettings = createMongoClientSettings();
        }
        catch (RuntimeException | Error initializationException) {
            try {
                close();
            }
            catch (RuntimeException | Error resourceReleasingException) {
                initializationException.addSuppressed(resourceReleasingException);
            }
            throw initializationException;
        }
    }

    @Override
    public MongoClientSettings getMongoClientSettings() {
        if (closed) {
            throw new IllegalStateException("MongoDB client factory is closed");
        }
        return clientSettings;
    }

    @Override
    public synchronized MongoDbClient openClient() {
        return openClient(null);
    }

    @Override
    public synchronized MongoDbClient openClient(ClusterListener listener) {
        final var client = listener == null ? getMongoClient() : getMongoClient(listener);
        activeClients++;
        return new MongoDbClient(client, this::clientClosed);
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (activeClients == 0) {
                closeAuthProvider();
            }
        }
    }

    private synchronized void clientClosed() {
        activeClients--;
        if (closed && activeClients == 0) {
            closeAuthProvider();
        }
    }

    private void closeAuthProvider() {
        final boolean interrupted = Thread.interrupted();
        try {
            authProvider.close();
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected MongoClientSettings createMongoClientSettings() {
        var sslContext = MongoDbClientFactory.createSSLContext(connectorConfig);

        // 1. apply property configuration
        var settings = MongoClientSettings.builder()
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(connectorConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(connectorConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(connectorConfig.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(connectorConfig.getHeartbeatFrequencyMs(), TimeUnit.MILLISECONDS))
                .applyToSslSettings(builder -> builder
                        .enabled(connectorConfig.isSslEnabled())
                        .invalidHostNameAllowed(connectorConfig.isSslAllowInvalidHostnames())
                        .context(sslContext));

        // 2. apply auth provider configuration
        authProvider.addAuthConfig(settings);

        // 3. apply connection string configuration
        settings.applyConnectionString(connectorConfig.getConnectionString());

        // build
        return settings.build();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import java.util.concurrent.TimeUnit;

import com.mongodb.MongoClientSettings;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.connection.MongoDbAuthProvider;

public class DefaultMongoDbClientFactory implements MongoDbClientFactory {

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoClientSettings clientSettings;
    private final MongoDbAuthProvider authProvider;

    public DefaultMongoDbClientFactory(Configuration config) {
        this.connectorConfig = new MongoDbConnectorConfig(config);
        this.authProvider = connectorConfig.getAuthProvider();
        this.authProvider.init(config);
        this.clientSettings = createMongoClientSettings();
    }

    @Override
    public MongoClientSettings getMongoClientSettings() {
        return clientSettings;
    }

    protected MongoClientSettings createMongoClientSettings() {
        var sslContext = MongoDbClientFactory.createSSLContext(connectorConfig);

        // 1. apply property configuration
        var settings = MongoClientSettings.builder()
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(connectorConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(connectorConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(connectorConfig.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(connectorConfig.getHeartbeatFrequencyMs(), TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(connectorConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(connectorConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToClusterSettings(builder -> builder
                        .serverSelectionTimeout(connectorConfig.getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS))
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

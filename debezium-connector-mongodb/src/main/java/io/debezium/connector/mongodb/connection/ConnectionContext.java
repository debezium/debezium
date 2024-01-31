/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;
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

        final MongoDbAuthProvider authProvider = config.getInstance(MongoDbConnectorConfig.AUTH_PROVIDER_CLASS, MongoDbAuthProvider.class);

        final int connectTimeoutMs = config.getInteger(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS);
        final int heartbeatFrequencyMs = config.getInteger(MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS);
        final int socketTimeoutMs = config.getInteger(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS);
        final int serverSelectionTimeoutMs = config.getInteger(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS);

        authProvider.init(config);

        // Set up the client pool so that it ...
        clientFactory = MongoDbClientFactory.create(settings -> {
            settings
                    .applyToSocketSettings(builder -> builder
                            .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                            .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToServerSettings(builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS))
                    .applyToSocketSettings(builder -> builder
                            .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                            .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToClusterSettings(builder -> builder
                            .serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                    .applyToSslSettings(builder -> builder
                            .enabled(connectorConfig.isSslEnabled())
                            .invalidHostNameAllowed(connectorConfig.isSslAllowInvalidHostnames())
                            .context(createSSLContext(connectorConfig)));
            authProvider.addAuthConfig(settings);
        });
    }

    /**
     * Creates keystore
     *
     * @param type     keyfile type
     * @param path     keyfile path
     * @param password keyfile password
     * @return keystore with loaded keys
     */
    static KeyStore loadKeyStore(String type, Path path, char[] password) {
        try (var keys = Files.newInputStream(path)) {
            var ks = KeyStore.getInstance(type);
            ks.load(keys, password);
            return ks;
        }
        catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            LOGGER.error("Unable to read key file from '{}'", path);
            throw new DebeziumException(e);
        }
    }

    /**
     * Creates SSL context initialized with custom
     *
     * @param connectorConfig connector configuration
     * @return ssl context
     */
    static SSLContext createSSLContext(MongoDbConnectorConfig connectorConfig) {
        try {
            var ksPath = connectorConfig.getSslKeyStore();
            var ksPass = connectorConfig.getSslKeyStorePassword();
            var ksType = connectorConfig.getSslKeyStoreType();
            KeyManager[] keyManagers = null;

            // Create keystore when configured
            if (ksPath.isPresent()) {
                var ks = loadKeyStore(ksType, ksPath.get(), ksPass);
                var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, ksPass);
                keyManagers = kmf.getKeyManagers();
            }

            // Create truststore when configured
            var tsPath = connectorConfig.getSslTrustStore();
            var tsPass = connectorConfig.getSslTrustStorePassword();
            var tsType = connectorConfig.getSslTrustStoreType();
            TrustManager[] trustManagers = null;

            if (tsPath.isPresent()) {
                var ts = loadKeyStore(tsType, tsPath.get(), tsPass);
                var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            // Create and initialize SSL context
            var context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, null);

            return context;
        }
        catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException e) {
            LOGGER.error("Unable to crate KeyStore/TrustStore manager factory");
            throw new DebeziumException(e);
        }
        catch (KeyManagementException e) {
            LOGGER.error("Unable to initialize SSL context");
            throw new DebeziumException(e);
        }
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

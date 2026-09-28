/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.function.Consumer;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.internal.MongoClientImpl;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;

public interface MongoDbClientFactory {

    Logger LOGGER = LoggerFactory.getLogger(MongoDbClientFactory.class);

    /**
     * Creates {@link MongoClientSettings} used to obtain {@link MongoClient} instances
     *
     * @return client settings
     */
    MongoClientSettings getMongoClientSettings();

    /**
     * Creates a factory adapter for a driver-native client.
     *
     * @param client the MongoDB client
     * @return an adapter when the client exposes the native driver settings, otherwise empty
     */
    static Optional<MongoDbClientFactory> adapt(MongoClient client) {
        if (!(client instanceof MongoClientImpl mongoClient)) {
            return Optional.empty();
        }

        return Optional.of(new MongoDbClientFactory() {
            @Override
            public MongoClientSettings getMongoClientSettings() {
                return mongoClient.getSettings();
            }

            @Override
            public MongoClient getMongoClient() {
                return mongoClient;
            }
        });
    }

    /**
     * Creates native {@link MongoClient} instance
     *
     * @return mongo client
     */
    default MongoClient getMongoClient() {
        var clientSettings = getMongoClientSettings();
        return MongoClients.create(clientSettings);
    }

    /**
     * Creates a native client for metadata operations that require access to driver cursor execution.
     * Custom factories may override this method when their client settings cannot fully describe their connection.
     *
     * The returned client must be a driver-native client created by {@link MongoClients}.
     *
     * @return driver-native metadata client
     */
    default MongoClient getMetadataMongoClient() {
        return MongoClients.create(getMongoClientSettings());
    }

    /**
     * Performs an operation for every database name.
     *
     * @param client the MongoDB client
     * @param operation the operation to perform for every database name
     */
    default void forEachDatabaseName(MongoClient client, Consumer<String> operation) {
        MongoDbClientOperations.forEachDatabaseName(client, this::getMetadataMongoClient, operation);
    }

    /**
     * Performs an operation for every collection name in the given database.
     * <p>
     * Custom factories that cannot recreate an equivalent client from {@link #getMongoClientSettings()} may override this method
     * to provide their own read-preference-aware cursor implementation.
     *
     * @param client the MongoDB client
     * @param databaseName the database name
     * @param operation the operation to perform for every collection name
     */
    default void forEachCollectionNameInDatabase(MongoClient client, String databaseName, Consumer<String> operation) {
        MongoDbClientOperations.forEachCollectionNameInDatabase(
                client,
                this::getMetadataMongoClient,
                databaseName,
                operation);
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
}

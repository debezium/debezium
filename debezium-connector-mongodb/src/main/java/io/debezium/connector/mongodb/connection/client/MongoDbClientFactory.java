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
import com.mongodb.event.ClusterListener;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;

public interface MongoDbClientFactory extends AutoCloseable {

    @Override
    default void close() {
    }

    Logger LOGGER = LoggerFactory.getLogger(MongoDbClientFactory.class);

    /**
     * Creates {@link MongoClientSettings} used to obtain {@link MongoClient} instances
     *
     * @return client settings
     */
    MongoClientSettings getMongoClientSettings();

    /**
     * Creates a native {@link MongoClient} instance. The caller must close it before closing this factory.
     * Use {@link #openClient()} when factory shutdown can overlap with client use.
     *
     * @return mongo client
     */
    default MongoClient getMongoClient() {
        var clientSettings = getMongoClientSettings();
        return MongoClients.create(clientSettings);
    }

    /**
     * Creates a client with an additional cluster listener, preserving the configured listeners.
     */
    default MongoClient getMongoClient(ClusterListener listener) {
        return MongoClients.create(MongoClientSettings.builder(getMongoClientSettings())
                .applyToClusterSettings(builder -> builder.addClusterListener(listener))
                .build());
    }

    /**
     * Opens a client whose close operation preserves the calling thread's interrupt status.
     * Implementations can defer factory resource cleanup until the client closes.
     */
    default MongoDbClient openClient() {
        return new MongoDbClient(getMongoClient());
    }

    default MongoDbClient openClient(ClusterListener listener) {
        return new MongoDbClient(getMongoClient(listener));
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

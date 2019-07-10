/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.network;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public class SslContextFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SslContextFactory.class);
    private SslContextFactory() { }

    /**
     * Return an {@link SslContext} containing all SSL configurations parsed
     * from the YAML file path
     * <p>
     * See {@link SslConfig} class for a list of valid config names
     *
     * @param sslConfigPath the SSL config file path required for the storage node
     * @return SslContext
     */
    public static SslContext createSslContext(String sslConfigPath) throws GeneralSecurityException, IOException {
        if (sslConfigPath == null) {
            throw new CassandraConnectorConfigException("Please specify SSL config path in cdc.yml");
        }
        Yaml yaml = new Yaml();
        try (FileInputStream fis = new FileInputStream(sslConfigPath)) {
            SslConfig sslConfig = new SslConfig(yaml.load(fis));
            return createSslContext(sslConfig);
        }
    }

    public static SslContext createSslContext(SslConfig config) throws GeneralSecurityException, IOException {
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();

            if (config.keyStoreLocation() != null) {
                KeyStore keyStore = KeyStore.getInstance(config.keyStoreType());
                try (FileInputStream is = new FileInputStream(config.keyStoreLocation())) {
                    keyStore.load(is, config.keyStorePassword().toCharArray());
                } catch (IOException ex) {
                    LOGGER.error("failed to load the key store: location=" + config.keyStoreLocation() + " type=" + config.keyStoreType());
                    throw ex;
                }
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(config.getKeyManagerAlgorithm());
                keyManagerFactory.init(keyStore, config.keyStorePassword().toCharArray());

                builder = SslContextBuilder.forClient();
                builder.keyManager(keyManagerFactory);

            } else {
                LOGGER.error("KeyStoreLocation was not specified. Building SslContext without certificate. This is not suitable for PRODUCTION");
                final SelfSignedCertificate ssc = new SelfSignedCertificate();

                builder = builder.keyManager(ssc.certificate(), ssc.privateKey());
            }

            if (config.trustStoreLocation() != null) {
                KeyStore trustStore = KeyStore.getInstance(config.trustStoreType());
                try (FileInputStream is = new FileInputStream(config.trustStoreLocation())) {
                    trustStore.load(is, config.trustStorePassword().toCharArray());
                } catch (IOException ex) {
                    LOGGER.error("failed to load the trust store: location=" + config.trustStoreLocation() + " type=" + config.trustStoreType());
                    throw ex;
                }
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(config.trustManagerAlgorithm());
                trustManagerFactory.init(trustStore);

                builder.trustManager(trustManagerFactory);

            } else {
                LOGGER.error("TrustStoreLocation was not specified. Building SslContext using InsecureTrustManagerFactory. This is not suitable for PRODUCTION");
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }

            return builder.build();

        } catch (GeneralSecurityException | IOException e) {
            LOGGER.error("Failed to create SslContext", e);
            throw e;
        }
    }
}

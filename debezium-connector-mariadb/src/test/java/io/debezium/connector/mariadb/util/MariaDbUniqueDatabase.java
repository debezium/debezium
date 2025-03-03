/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import java.net.URL;
import java.time.ZoneId;
import java.util.Map;
import java.util.Random;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.jdbc.JdbcConnection;

/**
 * An implementation of {@link UniqueDatabase} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbUniqueDatabase extends UniqueDatabase {

    public MariaDbUniqueDatabase(String serverName, String databaseName) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), null);
    }

    public MariaDbUniqueDatabase(String serverName, String databaseName, String identifier, String charSet) {
        super(serverName, databaseName, identifier, charSet);
    }

    @Override
    protected JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        return MariaDbTestConnection.forTestDatabase(databaseName, urlProperties);
    }

    @Override
    public Configuration.Builder defaultJdbcConfigBuilder() {
        Configuration.Builder builder = super.defaultJdbcConfigBuilder();
        String sslMode = System.getProperty("database.ssl.mode", "disable");
        if (sslMode.equals("disable")) {
            builder.with(MariaDbConnectorConfig.SSL_MODE, MariaDbConnectorConfig.MariaDbSecureConnectionMode.DISABLE);
        }
        else {
            URL trustStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/truststore");
            URL keyStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/keystore");

            builder.with(MariaDbConnectorConfig.SSL_MODE, sslMode)
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE, System.getProperty("database.ssl.truststore", trustStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD, System.getProperty("database.ssl.truststore.password", "debezium"))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE, System.getProperty("database.ssl.keystore", keyStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD, System.getProperty("database.ssl.keystore.password", "debezium"));
        }

        return builder;
    }

    @Override
    public ZoneId getTimezone() {
        return ZoneId.of("UTC");
    }

}

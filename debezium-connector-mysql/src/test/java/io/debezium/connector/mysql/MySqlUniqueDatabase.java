/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.net.URL;
import java.time.ZoneId;
import java.util.Map;
import java.util.Random;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.jdbc.JdbcConnection;

/**
 * An implementation of {@link UniqueDatabase} for MySQL.
 *
 * @author jpechane
 */
public class MySqlUniqueDatabase extends UniqueDatabase {

    public MySqlUniqueDatabase(String serverName, String databaseName) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), null);
    }

    public MySqlUniqueDatabase(String serverName, String databaseName, String identifier, String charSet) {
        super(serverName, databaseName, identifier, charSet);
    }

    @Override
    protected JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        return MySqlTestConnection.forTestDatabase(databaseName, urlProperties);
    }

    @Override
    public Configuration.Builder defaultJdbcConfigBuilder() {
        Builder builder = super.defaultJdbcConfigBuilder();
        builder.with(MySqlConnectorConfig.JDBC_PROTOCOL,
                System.getProperty("database.protocol",
                        MySqlConnectorConfig.JDBC_PROTOCOL.defaultValueAsString()))
                .with(MySqlConnectorConfig.JDBC_DRIVER,
                        System.getProperty("database.jdbc.driver",
                                MySqlConnectorConfig.JDBC_DRIVER.defaultValueAsString()));

        String sslMode = System.getProperty("database.ssl.mode", "preferred");
        if (sslMode.equals("disabled")) {
            builder.with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.MySqlSecureConnectionMode.DISABLED);
        }
        else {
            URL trustStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/truststore");
            URL keyStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/keystore");

            builder.with(MySqlConnectorConfig.SSL_MODE, sslMode)
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE, System.getProperty("database.ssl.truststore", trustStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD, System.getProperty("database.ssl.truststore.password", "debezium"))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE, System.getProperty("database.ssl.keystore", keyStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD, System.getProperty("database.ssl.keystore.password", "debezium"));
        }

        return builder;
    }

    @Override
    public ZoneId getTimezone() {
        return ZoneId.of("US/Samoa");
    }
}

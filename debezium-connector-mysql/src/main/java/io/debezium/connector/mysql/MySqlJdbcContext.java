/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;

/**
 * A context for a JDBC connection to MySQL.
 * 
 * @author Randall Hauch
 */
public class MySqlJdbcContext implements AutoCloseable {

    protected static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=${useSSL}";
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL);

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final Configuration config;
    protected final JdbcConnection jdbc;
    private final Map<String,String> originalSystemProperties = new HashMap<>();

    public MySqlJdbcContext(Configuration config) {
        this.config = config; // must be set before most methods are used

        // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
        // to give us better JDBC database metadata behavior ...
        boolean useSSL = sslModeEnabled();
        Configuration jdbcConfig = config.subset("database.", true)
                                         .edit()
                                         .with("useInformationSchema", "true")
                                         .with("nullCatalogMeansCurrent", "false")
                                         .with("useSSL", Boolean.toString(useSSL))
                                         .build();
        this.jdbc = new JdbcConnection(jdbcConfig, FACTORY);
    }

    public Configuration config() {
        return config;
    }

    public JdbcConnection jdbc() {
        return jdbc;
    }

    public Logger logger() {
        return logger;
    }

    public String username() {
        return config.getString(MySqlConnectorConfig.USER);
    }

    public String password() {
        return config.getString(MySqlConnectorConfig.PASSWORD);
    }

    public String hostname() {
        return config.getString(MySqlConnectorConfig.HOSTNAME);
    }

    public int port() {
        return config.getInteger(MySqlConnectorConfig.PORT);
    }

    public SecureConnectionMode sslMode() {
        String mode = config.getString(MySqlConnectorConfig.SSL_MODE);
        return SecureConnectionMode.parse(mode);
    }
    
    public boolean sslModeEnabled() {
        return sslMode() != SecureConnectionMode.DISABLED;
    }
    
    public void start() {
        if (sslModeEnabled()) {
            originalSystemProperties.clear();
            // Set the System properties for SSL for the MySQL driver ...
            setSystemProperty("javax.net.ssl.keyStore", MySqlConnectorConfig.SSL_KEYSTORE, true);
            setSystemProperty("javax.net.ssl.keyStorePassword", MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, false);
            setSystemProperty("javax.net.ssl.trustStore", MySqlConnectorConfig.SSL_TRUSTSTORE, true);
            setSystemProperty("javax.net.ssl.trustStorePassword", MySqlConnectorConfig.SSL_KEYSTORE_PASSWORD, false);
        }
    }

    public void shutdown() {
        try {
            jdbc.close();
        } catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
        } finally {
            // Reset the system properties to their original value ...
            originalSystemProperties.forEach((name,value)->{
                if ( value != null ) {
                    System.setProperty(name,value);
                } else {
                    System.clearProperty(name);
                }
            });
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    protected String connectionString() {
        return jdbc.connectionString(MYSQL_CONNECTION_URL);
    }

    protected void setSystemProperty(String property, Field field, boolean showValueInError) {
        String value = config.getString(field);
        if (value != null) {
            String existingValue = System.getProperty(property);
            if (existingValue != null && existingValue.equalsIgnoreCase(value)) {
                String msg = "System or JVM property '" + property + "' is already defined, but the configuration property '" + field.name()
                        + "' defines a different value";
                if (showValueInError) {
                    msg = "System or JVM property '" + property + "' is already defined as " + existingValue
                            + ", but the configuration property '" + field.name() + "' defines a different value '" + value + "'";
                }
                throw new ConnectException(msg);
            } else {
                String existing = System.setProperty(property, value);
                originalSystemProperties.put(property, existing); // the existing value may be null
            }
        }
    }
}

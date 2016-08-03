/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;

/**
 * A context for a JDBC connection to MySQL.
 * 
 * @author Randall Hauch
 */
public class MySqlJdbcContext implements AutoCloseable {

    protected static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false";
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL);

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final Configuration config;
    protected final JdbcConnection jdbc;

    public MySqlJdbcContext(Configuration config) {
        this.config = config; // must be set before most methods are used

        // Set up the JDBC connection without actually connecting, with extra MySQL-specific properties
        // to give us better JDBC database metadata behavior ...
        Configuration jdbcConfig = config.subset("database.", true)
                                         .edit()
                                         .with("useInformationSchema", "true")
                                         .with("nullCatalogMeansCurrent", "false")
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

    public void start() {
    }

    public void shutdown() {
        try {
            jdbc.close();
        } catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
        }
    }

    @Override
    public void close() {
        shutdown();
    }
    
    protected String connectionString() {
        return jdbc.connectionString(MYSQL_CONNECTION_URL);
    }
}

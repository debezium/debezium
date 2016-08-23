/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgres;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A utility for integration test cases to connect the PostgreSQL server running in the Docker container created by this module's
 * build.
 * 
 * @author Randall Hauch
 */
public class PostgresConnection extends JdbcConnection {
    
    /**
     * Obtain a connection instance to the named test database.
     *

     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection forTestDatabase() {
        return new PostgresConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                                       .build());
    }
   
    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the name of the test database
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection forTestDatabase(String databaseName) {
        return new PostgresConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                                       .withDatabase(databaseName)
                                                       .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     * 
     * @param databaseName the name of the test database
     * @param username the username
     * @param password the password
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection forTestDatabase(String databaseName, String username, String password) {
        return new PostgresConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                                       .withDatabase(databaseName)
                                                       .withUser(username)
                                                       .withPassword(password)
                                                       .build());
    }

    protected static void addDefaults(Configuration.Builder builder) {
        builder.withDefault(JdbcConfiguration.HOSTNAME, "localhost")
               .withDefault(JdbcConfiguration.PORT, 5432)
               .withDefault(JdbcConfiguration.USER, "postgres")
               .withDefault(JdbcConfiguration.PASSWORD, "postgres");
    }

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:postgresql://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     * 
     * @param config the configuration; may not be null
     */
    protected PostgresConnection(Configuration config) {
        super(config, FACTORY, null, PostgresConnection::addDefaults);
    }
}

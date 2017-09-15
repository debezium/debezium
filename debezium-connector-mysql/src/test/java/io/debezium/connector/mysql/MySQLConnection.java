/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A utility for integration test cases to connect the MySQL server running in the Docker container created by this module's
 * build.
 * 
 * @author Randall Hauch
 */
public class MySQLConnection extends JdbcConnection {

    /**
     * Obtain a connection instance to the named test database.
     * 
     * @param databaseName the name of the test database
     * @return the MySQLConnection instance; never null
     */
    public static MySQLConnection forTestDatabase(String databaseName) {
        return new MySQLConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                                    .withDatabase(databaseName)
                                                    .with("useSSL", false)
                                                    .with("characterEncoding", "utf8")
                                                    .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     * 
     * @param databaseName the name of the test database
     * @param username the username
     * @param password the password
     * @return the MySQLConnection instance; never null
     */
    public static MySQLConnection forTestDatabase(String databaseName, String username, String password) {
        return new MySQLConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                                                    .withDatabase(databaseName)
                                                    .withUser(username)
                                                    .withPassword(password)
                                                    .with("useSSL", false)
                                                    .build());
    }

    protected static void addDefaults(Configuration.Builder builder) {
        builder.withDefault(JdbcConfiguration.HOSTNAME, "localhost")
               .withDefault(JdbcConfiguration.PORT, 3306)
               .withDefault(JdbcConfiguration.USER, "mysqluser")
               .withDefault(JdbcConfiguration.PASSWORD, "mysqlpw");
    }

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:mysql://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     * 
     * @param config the configuration; may not be null
     */
    public MySQLConnection(Configuration config) {
        super(config, FACTORY, null, MySQLConnection::addDefaults);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     * 
     * @param config the configuration; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     */
    public MySQLConnection(Configuration config, Operations initialOperations) {
        super(config, FACTORY, initialOperations, MySQLConnection::addDefaults);
    }
}

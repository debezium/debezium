/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.Map;

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

    public enum MySqlVersion {
        MYSQL_5,
        MYSQL_8;
    }

    private DatabaseDifferences databaseAsserts;
    private MySqlVersion mySqlVersion;

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
     * @param databaseName the name of the test database
     * @param urlProperties url properties
     * @return the MySQLConnection instance; never null
     */
    public static MySQLConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        JdbcConfiguration.Builder builder = JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName)
                .with("useSSL", false)
                .with("characterEncoding", "utf8");
        urlProperties.forEach(builder::with);
        return new MySQLConnection(builder.build());
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

    public MySqlVersion getMySqlVersion() {
        if (mySqlVersion == null) {
            String versionString;
            try {
                versionString = connect().queryAndMap("SHOW GLOBAL VARIABLES LIKE 'version'", rs -> {
                    rs.next();
                    return rs.getString(2);
                });

                mySqlVersion = versionString.startsWith("8.") ? MySqlVersion.MYSQL_8 : MySqlVersion.MYSQL_5;
            }
            catch (SQLException e) {
                throw new IllegalStateException("Couldn't obtain MySQL Server version", e);
            }
        }

        return mySqlVersion;
    }

    public DatabaseDifferences databaseAsserts() {
        if (databaseAsserts == null) {
            if (getMySqlVersion() == MySqlVersion.MYSQL_8) {
                databaseAsserts = new DatabaseDifferences() {
                    @Override
                    public boolean isCurrentDateTimeDefaultGenerated() {
                        return true;
                    }

                    @Override
                    public String currentDateTimeDefaultOptional(String isoString) {
                        return null;
                    }
                };
            }
            else {
                databaseAsserts = new DatabaseDifferences() {
                    @Override
                    public boolean isCurrentDateTimeDefaultGenerated() {
                        return false;
                    }

                    @Override
                    public String currentDateTimeDefaultOptional(String isoString) {
                        return isoString;
                    }

                };
            }
        }
        return databaseAsserts;
    }
}

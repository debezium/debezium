/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;

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
public class MySqlTestConnection extends JdbcConnection {

    public enum MySqlVersion {
        MYSQL_5_5,
        MYSQL_5_6,
        MYSQL_5_7,
        MYSQL_8,
        MARIADB_11;
    }

    private DatabaseDifferences databaseAsserts;
    private MySqlVersion mySqlVersion;

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the name of the test database
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestDatabase(String databaseName) {
        return new MySqlTestConnection(JdbcConfiguration.copy(
                Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX).merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)))
                .withDatabase(databaseName)
                .with("characterEncoding", "utf8")
                .build());
    }

    /**
     * Obtain a connection instance to the named test replica database.
     * if no replica, obtain same connection with {@link #forTestDatabase(String) forTestDatabase}
     * @param databaseName the name of the test replica database
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestReplicaDatabase(String databaseName) {
        return new MySqlTestConnection(JdbcConfiguration.copy(
                Configuration.fromSystemProperties("database.replica.").merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)
                        .merge(Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX))))
                .withDatabase(databaseName)
                .with("characterEncoding", "utf8")
                .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     * @param databaseName the name of the test database
     * @param urlProperties url properties
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        JdbcConfiguration.Builder builder = JdbcConfiguration.copy(
                Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX).merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)))
                .withDatabase(databaseName)
                .with("characterEncoding", "utf8");
        urlProperties.forEach(builder::with);
        return new MySqlTestConnection(builder.build());
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the name of the test database
     * @param username the username
     * @param password the password
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestDatabase(String databaseName, String username, String password) {
        return new MySqlTestConnection(JdbcConfiguration.copy(
                Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX).merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)))
                .withDatabase(databaseName)
                .withUser(username)
                .withPassword(password)
                .build());
    }

    /**
     * Obtain whether the database source is MySQL 5.x or not.
     *
     * @return true if the database version is 5.x; otherwise false.
     */
    public static boolean isMySQL5() {
        switch (forTestDatabase("mysql").getMySqlVersion()) {
            case MYSQL_5_5:
            case MYSQL_5_6:
            case MYSQL_5_7:
                return true;
            default:
                return false;
        }
    }

    /**
     * Obtain whether the database source is the Percona Server fork.
     *
     * @return true if the database is Percona Server; otherwise false.
     */
    public static boolean isPerconaServer() {
        String comment = forTestDatabase("mysql").getMySqlVersionComment();
        return comment.startsWith("Percona");
    }

    /**
     * Check whether the database is MariaDB or MySQL.
     *
     * @return true if the database is MariaDB; otherwise false
     */
    public static boolean isMariaDb() {
        try (MySqlTestConnection connection = forTestDatabase("mysql")) {
            return connection.isVersionCommentMariaDb();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve if database is MariaDB", e);
        }
    }

    /**
     * Checks whether the database version string indicates MariaDB.
     *
     * @return true if the database is MariaDB; otherwise false
     */
    public boolean isVersionCommentMariaDb() {
        return getMySqlVersionComment().toLowerCase().contains("mariadb");
    }

    private static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration) {
        return JdbcConfiguration.adapt(configuration.edit()
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 3306)
                .withDefault(JdbcConfiguration.USER, "mysqluser")
                .withDefault(JdbcConfiguration.PASSWORD, "mysqlpw")
                .build());

    }

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("${protocol}://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     *
     * @param config the configuration; may not be null
     */
    public MySqlTestConnection(JdbcConfiguration config) {
        super(addDefaultSettings(config), FACTORY, "`", "`");
    }

    public MySqlVersion getMySqlVersion() {
        if (mySqlVersion == null) {
            final String versionString = getMySqlVersionString();

            if (isVersionCommentMariaDb()) {
                if (versionString.startsWith("11.")) {
                    mySqlVersion = MySqlVersion.MARIADB_11;
                }
                else {
                    throw new IllegalStateException("Couldn't resolve MariaDB Server version");
                }
                return mySqlVersion;
            }

            // Fallback to MySQL
            if (versionString.startsWith("8.")) {
                mySqlVersion = MySqlVersion.MYSQL_8;
            }
            else if (versionString.startsWith("5.5")) {
                mySqlVersion = MySqlVersion.MYSQL_5_5;
            }
            else if (versionString.startsWith("5.6")) {
                mySqlVersion = MySqlVersion.MYSQL_5_6;
            }
            else if (versionString.startsWith("5.7")) {
                mySqlVersion = MySqlVersion.MYSQL_5_7;
            }
            else {
                throw new IllegalStateException("Couldn't resolve MySQL Server version");
            }
        }

        return mySqlVersion;
    }

    public String getMySqlVersionString() {
        String versionString;
        try {
            versionString = connect().queryAndMap("SHOW GLOBAL VARIABLES LIKE 'version'", rs -> {
                rs.next();
                return rs.getString(2);
            });
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't obtain MySQL Server version", e);
        }
        return versionString;
    }

    public String getMySqlVersionComment() {
        String versionString;
        try {
            versionString = connect().queryAndMap("SHOW GLOBAL VARIABLES LIKE 'version_comment'", rs -> {
                rs.next();
                return rs.getString(2);
            });
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't obtain MySQL Server version comment", e);
        }
        return versionString;
    }

    public boolean isTableIdCaseSensitive() {
        String caseString;
        try {
            caseString = connect().queryAndMap("SHOW GLOBAL VARIABLES LIKE '" + MySqlSystemVariables.LOWER_CASE_TABLE_NAMES + "'", rs -> {
                rs.next();
                return rs.getString(2);
            });
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't obtain MySQL Server version comment", e);
        }
        return !"0".equals(caseString);
    }

    public DatabaseDifferences databaseAsserts() {
        if (databaseAsserts == null) {
            if (getMySqlVersion() == MySqlVersion.MARIADB_11) {
                databaseAsserts = new DatabaseDifferences() {
                    @Override
                    public boolean isCurrentDateTimeDefaultGenerated() {
                        return false;
                    }

                    @Override
                    public String currentDateTimeDefaultOptional(String isoString) {
                        return null;
                    }

                    @Override
                    public void setBinlogRowQueryEventsOff(JdbcConnection connection) throws SQLException {
                        connection.execute("SET binlog_annotate_row_events=OFF");
                    }

                    @Override
                    public void setBinlogRowQueryEventsOn(JdbcConnection connection) throws SQLException {
                        connection.execute("SET binlog_annotate_row_events=ON");
                    }

                    @Override
                    public void setBinlogCompressionOff(JdbcConnection connection) throws SQLException {
                        connection.execute("set global log_bin_compress=OFF;");
                    }

                    @Override
                    public void setBinlogCompressionOn(JdbcConnection connection) throws SQLException {
                        connection.execute("set global log_bin_compress=ON;");
                    }
                };
            }
            else if (getMySqlVersion() == MySqlVersion.MYSQL_8) {
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.Map;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A utility for integration test cases to connect the MySQL server running in the Docker container created by this module's
 * build.
 *
 * @author Randall Hauch
 */
public class MySqlTestConnection extends BinlogTestConnection {

    public enum MySqlVersion {
        MYSQL_5_5,
        MYSQL_5_6,
        MYSQL_5_7,
        MYSQL_8,
        MYSQL_9
    }

    MySqlVersion mySqlVersion;

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the name of the test database
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestDatabase(String databaseName) {
        return new MySqlTestConnection(getDefaultJdbcConfig(databaseName).build());
    }

    /**
     * Obtain a connection instance to the named test replica database.
     * if no replica, obtain same connection with {@link #forTestDatabase(String, int) forTestDatabase}
     * @param databaseName the name of the test replica database
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestReplicaDatabase(String databaseName) {
        return new MySqlTestConnection(getReplicaJdbcConfig(databaseName).build());
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     *
     * @param databaseName the name of the test database
     * @param queryTimeout the seconds to wait for query execution
     * @return the MySQLConnection instance; never null
     */

    public static MySqlTestConnection forTestDatabase(String databaseName, int queryTimeout) {
        return new MySqlTestConnection(getDefaultJdbcConfig(databaseName)
                .withQueryTimeoutMs(queryTimeout)
                .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     * @param databaseName the name of the test database
     * @param urlProperties url properties
     * @return the MySQLConnection instance; never null
     */
    public static MySqlTestConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        final JdbcConfiguration.Builder builder = getDefaultJdbcConfig(databaseName);
        urlProperties.forEach(builder::with);
        return new MySqlTestConnection(builder.build());
    }

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("${protocol}://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     *
     * @param config the configuration; may not be null
     */
    public MySqlTestConnection(JdbcConfiguration config) {
        super(addDefaultSettings(config), FACTORY);
    }

    @Override
    public boolean isGtidEnabled() {
        return false; // not used
    }

    @Override
    public boolean isMariaDb() {
        return false;
    }

    @Override
    public boolean isMySQL5() {
        switch (getMySqlVersion()) {
            case MYSQL_5_5:
            case MYSQL_5_6:
            case MYSQL_5_7:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isPercona() {
        return getMySqlVersionString().startsWith("Percona");
    }

    @Override
    public String currentDateTimeDefaultOptional(String isoString) {
        return !MySqlVersion.MYSQL_8.equals(getMySqlVersion()) && !MySqlVersion.MYSQL_9.equals(getMySqlVersion())
                ? isoString
                : null;
    }

    @Override
    public void setBinlogCompressionOff() throws SQLException {
        execute("set binlog_transaction_compression=OFF;");
    }

    @Override
    public void setBinlogCompressionOn() throws SQLException {
        execute("set binlog_transaction_compression=ON;");
    }

    @Override
    public void setBinlogRowQueryEventsOff() throws SQLException {
        execute("SET binlog_rows_query_log_events=OFF");
    }

    @Override
    public void setBinlogRowQueryEventsOn() throws SQLException {
        execute("SET binlog_rows_query_log_events=ON");
    }

    @Override
    public boolean isCurrentDateTimeDefaultGenerated() {
        return MySqlVersion.MYSQL_8.equals(getMySqlVersion()) || MySqlVersion.MYSQL_9.equals(getMySqlVersion());
    }

    public MySqlVersion getMySqlVersion() {
        if (mySqlVersion == null) {
            final String versionString = getMySqlVersionString();

            // Fallback to MySQL
            if (versionString.startsWith("9.")) {
                mySqlVersion = MySqlVersion.MYSQL_9;
            }
            else if (versionString.startsWith("8.")) {
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

}

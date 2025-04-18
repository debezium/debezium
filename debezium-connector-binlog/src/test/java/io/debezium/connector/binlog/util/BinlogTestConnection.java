/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;
import static io.debezium.config.CommonConnectorConfig.DRIVER_CONFIG_PREFIX;
import static io.debezium.connector.binlog.jdbc.BinlogSystemVariables.LOWER_CASE_TABLE_NAMES;

import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public abstract class BinlogTestConnection extends JdbcConnection {

    public BinlogTestConnection(JdbcConfiguration config, ConnectionFactory factory) {
        super(addDefaultSettings(config), factory, "`", "`");
    }

    /**
     * @return true if table names are case-sensitive, false otherwise
     */
    public boolean isTableIdCaseSensitive() {
        String caseString;
        try {
            caseString = connect().queryAndMap("SHOW GLOBAL VARIABLES LIKE '" + LOWER_CASE_TABLE_NAMES + "'", rs -> {
                rs.next();
                return rs.getString(2);
            });
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't obtain MySQL Server version comment", e);
        }
        return !"0".equals(caseString);
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
            throw new IllegalStateException("Couldn't obtain MySQL Server version", e);
        }
        return versionString;
    }

    public String binaryLogStatusStatement() {
        final var binaryLogStatus = "SHOW BINARY LOG STATUS";
        try {
            query(binaryLogStatus, rs -> {
            });
            return binaryLogStatus;
        }
        catch (SQLException e) {
            return BinlogConnectorConnection.MASTER_STATUS_STATEMENT;
        }
    }

    public abstract boolean isGtidEnabled();

    /**
     * Returns whether the database is MariaDB.
     * @return true if MariaDB, false otherwise.
     */
    public abstract boolean isMariaDb();

    /**
     * Return whether the database is MySQL.
     * @return true if is MySQL 5, false otherwise.
     */
    public abstract boolean isMySQL5();

    /**
     * Returns whether the database is Percona.
     * @return true if Percona, false otherwise.
     */
    public abstract boolean isPercona();

    /**
     * Get the current date time default value
     * @param isoString the ISO string value
     * @return the default value
     */
    public abstract String currentDateTimeDefaultOptional(String isoString);

    /**
     * Set the binlog compression off.
     * @throws SQLException if a database exception occurs.
     */
    public abstract void setBinlogCompressionOff() throws SQLException;

    /**
     * Set the binlog compression on.
     * @throws SQLException if a database exception occurs.
     */
    public abstract void setBinlogCompressionOn() throws SQLException;

    /**
     * Set the binlog row query events off.
     * @throws SQLException if a database exception occurs.
     */
    public abstract void setBinlogRowQueryEventsOff() throws SQLException;

    /**
     * Set the binlog row query events on.
     * @throws SQLException if a database exception occurs.
     */
    public abstract void setBinlogRowQueryEventsOn() throws SQLException;

    /**
     * @return whether the current date time defaults as generated
     */
    public abstract boolean isCurrentDateTimeDefaultGenerated();

    protected static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration) {
        return JdbcConfiguration.adapt(configuration.edit()
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 3306)
                .withDefault(JdbcConfiguration.USER, "mysqluser")
                .withDefault(JdbcConfiguration.PASSWORD, "mysqlpw")
                .build());

    }

    protected static JdbcConfiguration.Builder getDefaultJdbcConfig(String databaseName) {
        return JdbcConfiguration.copy(
                Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX)
                        .merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)))
                .withDatabase(databaseName)
                .with("characterEncoding", "utf8");
    }

    protected static JdbcConfiguration.Builder getReplicaJdbcConfig(String databaseName) {
        return JdbcConfiguration.copy(
                Configuration.fromSystemProperties("database.replica.")
                        .merge(Configuration.fromSystemProperties(DRIVER_CONFIG_PREFIX)
                                .merge(Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX))))
                .withDatabase(databaseName)
                .with("characterEncoding", "utf8");
    }

}

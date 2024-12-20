/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import java.sql.SQLException;
import java.util.Map;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public class MariaDbTestConnection extends BinlogTestConnection {

    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:mariadb://${hostname}:${port}/${dbname}?sslMode=${ssl.mode}");

    /**
     * Create a new instance with the given configuration.
     *
     * @param config the configuration; may not be null
     */
    public MariaDbTestConnection(JdbcConfiguration config) {
        super(config, FACTORY);
    }

    @Override
    public boolean isGtidEnabled() {
        return true;
    }

    @Override
    public boolean isMariaDb() {
        return true;
    }

    @Override
    public boolean isMySQL5() {
        return false;
    }

    @Override
    public boolean isPercona() {
        return false;
    }

    @Override
    public String currentDateTimeDefaultOptional(String isoString) {
        return null;
    }

    @Override
    public void setBinlogCompressionOff() throws SQLException {
        execute("set global log_bin_compress=OFF;");
    }

    @Override
    public void setBinlogCompressionOn() throws SQLException {
        execute("set global log_bin_compress=ON;");
    }

    @Override
    public void setBinlogRowQueryEventsOff() throws SQLException {
        execute("set binlog_annotate_row_events=OFF;");
    }

    @Override
    public void setBinlogRowQueryEventsOn() throws SQLException {
        execute("set binlog_annotate_row_events=ON;");
    }

    @Override
    public boolean isCurrentDateTimeDefaultGenerated() {
        return false;
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the database name
     * @return the connection instance; never null
     */
    public static MariaDbTestConnection forTestDatabase(String databaseName) {
        return new MariaDbTestConnection(getDefaultJdbcConfig(databaseName).build());
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     *
     * @param databaseName the name of the test database
     * @param queryTimeout the seconds to wait for query execution
     * @return the connection instance; never null
     */

    public static MariaDbTestConnection forTestDatabase(String databaseName, int queryTimeout) {
        return new MariaDbTestConnection(getDefaultJdbcConfig(databaseName)
                .withQueryTimeoutMs(queryTimeout)
                .build());
    }

    /**
     * Obtain a connection instance to the named test database.
     *
     * @param databaseName the name of the test database
     * @param urlProperties url properties
     * @return the connection instance; never null
     */
    public static MariaDbTestConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties) {
        final JdbcConfiguration.Builder builder = getDefaultJdbcConfig(databaseName);
        urlProperties.forEach(builder::with);
        return new MariaDbTestConnection(builder.build());
    }

    public static MariaDbTestConnection forTestReplicaDatabase(String databaseName) {
        return new MariaDbTestConnection(getReplicaJdbcConfig(databaseName).build());
    }

}

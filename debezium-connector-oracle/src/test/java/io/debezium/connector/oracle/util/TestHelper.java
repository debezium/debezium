/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.nio.file.Path;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectionFactory;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

    public static final String CONNECTOR_USER = "c##xstrm";

    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, CONNECTOR_USER)
                .withDefault(JdbcConfiguration.PASSWORD, "xs")
                .withDefault(JdbcConfiguration.DATABASE, "ORCLCDB")
                .build();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder.with(OracleConnectorConfig.LOGICAL_NAME, "server1")
                .with(OracleConnectorConfig.PDB_NAME, "ORCLPDB1")
                .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout")
                .with(OracleConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    public static OracleConnection defaultConnection() {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());

        String pdbName = new OracleConnectorConfig(config).getPdbName();

        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        return jdbcConnection;
    }

    /**
     * Returns a JDBC configuration for the test data schema and user (NOT the XStream user).
     */
    private static JdbcConfiguration testJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, "debezium")
                .withDefault(JdbcConfiguration.PASSWORD, "dbz")
                .withDefault(JdbcConfiguration.DATABASE, "ORCLPDB1")
                .build();
    }

    private static Configuration.Builder testConfig() {
        JdbcConfiguration jdbcConfiguration = testJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder;
    }

    public static OracleConnection testConnection() {
        Configuration config = testConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String pdbName = new OracleConnectorConfig(config).getPdbName();

        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        return jdbcConnection;
    }

    public static void dropTable(OracleConnection connection, String table) {
        try {
            connection.execute("drop table " + table);
        }
        catch (SQLException e) {
            if (!e.getMessage().contains("table or view does not exist")) {
                throw new RuntimeException(e);
            }
        }
    }

    public static int defaultMessageConsumerPollTimeout() {
        return 120;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectionFactory;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, "c##xstrmadmin")
                .withDefault(JdbcConfiguration.PASSWORD, "xsa")
                .withDefault(JdbcConfiguration.DATABASE, "ORCLCDB")
                .build();
    }

    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        builder.with(OracleConnectorConfig.LOGICAL_NAME, "server1")
                .with(OracleConnectorConfig.PDB_NAME, "ORCLPDB1")
                .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout");

        return builder;
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
}

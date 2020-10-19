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
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

    public static final String CONNECTOR_USER = "c##xstrm";
    public static final String CONNECTOR_USER_LOGMINER = "c##dbzuser";

    public static final String CONNECTOR_NAME = "oracle";

    public static final String SERVER_NAME = "server1";

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    public static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    public static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    public static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    // todo: unify logminer and xstream user accounts
    public static final String CONNECTOR_USER_PASS = "xs";
    public static final String CONNECTOR_USER_PASS_LOGMINER = "dbz";
    public static final String HOST = "localhost";
    public static final String SCHEMA_USER = "debezium";
    public static final String SCHEMA_PASS = "dbz";
    public static final String DATABASE = "ORCLPDB1";
    public static final String DATABASE_CDB = "ORCLCDB";

    public static String getConnectorUserName() {
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            return CONNECTOR_USER_LOGMINER;
        }
        return CONNECTOR_USER;
    }

    public static String getConnectorUserPassword() {
        if (TestHelper.adapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            return CONNECTOR_USER_PASS_LOGMINER;
        }
        return CONNECTOR_USER_PASS;
    }

    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, getConnectorUserName())
                .withDefault(JdbcConfiguration.PASSWORD, getConnectorUserPassword())
                .withDefault(JdbcConfiguration.DATABASE, DATABASE_CDB)
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
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(OracleConnectorConfig.SERVER_NAME, SERVER_NAME)
                .with(OracleConnectorConfig.PDB_NAME, "ORCLPDB1")
                .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout")
                .with(OracleConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(OracleConnectorConfig.SCHEMA_NAME, SCHEMA_USER)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static OracleConnection defaultConnection() {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader);

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
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, SCHEMA_USER)
                .withDefault(JdbcConfiguration.PASSWORD, SCHEMA_PASS)
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    /**
     * Returns a JDBC configuration for database admin user (NOT the XStream user).
     */
    private static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database.admin."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, "sys as sysdba")
                .withDefault(JdbcConfiguration.PASSWORD, "top_secret")
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    private static Configuration.Builder testConfig() {
        JdbcConfiguration jdbcConfiguration = testJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder;
    }

    private static Configuration.Builder adminConfig() {
        JdbcConfiguration jdbcConfiguration = adminJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder;
    }

    public static OracleConnection testConnection() {
        Configuration config = testConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader);
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

    public static OracleConnection adminConnection() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader);
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
            connection.execute("DROP TABLE " + table);
        }
        catch (SQLException e) {
            if (!e.getMessage().contains("ORA-00942") || 942 != e.getErrorCode()) {
                throw new RuntimeException(e);
            }
        }
    }

    public static int defaultMessageConsumerPollTimeout() {
        return 120;
    }

    public static OracleConnectorConfig.ConnectorAdapter adapter() {
        final String s = System.getProperty(OracleConnectorConfig.CONNECTOR_ADAPTER.name());
        return (s == null || s.length() == 0) ? OracleConnectorConfig.ConnectorAdapter.XSTREAM : OracleConnectorConfig.ConnectorAdapter.parse(s);
    }
}

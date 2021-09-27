/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

public class TestHelper {

    private static final String PDB_NAME = "pdb.name";
    private static final String DATABASE_PREFIX = "database.";
    private static final String DATABASE_ADMIN_PREFIX = "database.admin.";

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

    public static final String CONNECTOR_USER = "c##dbzuser";
    public static final String CONNECTOR_NAME = "oracle";
    public static final String SERVER_NAME = "server1";
    public static final String CONNECTOR_USER_PASS = "dbz";
    public static final String HOST = "localhost";
    public static final String SCHEMA_USER = "debezium";
    public static final String SCHEMA_PASS = "dbz";
    public static final String DATABASE = "ORCLPDB1";
    public static final String DATABASE_CDB = "ORCLCDB";
    public static final int PORT = 1521;

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

    /**
     * Get the name of the connector user, the default is {@link TestHelper#CONNECTOR_USER}.
     */
    public static String getConnectorUserName() {
        final String userName = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.USER.name());
        return Strings.isNullOrEmpty(userName) ? CONNECTOR_USER : userName;
    }

    /**
     * Get the password of the connector user, the default is {@link TestHelper#CONNECTOR_USER_PASS}.
     */
    private static String getConnectorUserPassword() {
        final String password = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.PASSWORD.name());
        return Strings.isNullOrEmpty(password) ? CONNECTOR_USER_PASS : password;
    }

    /**
     * Get the database name, the default is {@link TestHelper#DATABASE}.
     */
    public static String getDatabaseName() {
        final String databaseName = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.DATABASE);
        return Strings.isNullOrEmpty(databaseName) ? DATABASE : databaseName;
    }

    /**
     * Returns a JdbcConfiguration that is specific for the XStream/LogMiner user accounts.
     * If connecting to a CDB enabled database, this connection is to the root database.
     */
    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(getDatabaseConfig(DATABASE_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, PORT)
                .withDefault(JdbcConfiguration.USER, getConnectorUserName())
                .withDefault(JdbcConfiguration.PASSWORD, getConnectorUserPassword())
                .withDefault(JdbcConfiguration.DATABASE, DATABASE_CDB)
                .build();
    }

    /**
     * Returns a JdbcConfiguration that is specific and suitable for initializing the connector.
     * The returned builder can be amended with values as a test case requires.
     *
     * When initializing a connector connection to a database that operates in non-CDB mode, the
     * configuration should still provide a {@code database.pdb.name} setting; however the value
     * of the setting should be empty.  This is specific to the test suite only.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        if (adapter().equals(OracleConnectorConfig.ConnectorAdapter.XSTREAM)) {
            builder.withDefault(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout");
        }
        else {
            // Tests will always use the online catalog strategy due to speed.
            builder.withDefault(OracleConnectorConfig.LOG_MINING_STRATEGY, "online_catalog");

            final String bufferType = System.getProperty(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE.name());
            if (LogMiningBufferType.parse(bufferType).equals(LogMiningBufferType.INFINISPAN)) {
                builder.withDefault(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, "infinispan");
                builder.withDefault(OracleConnectorConfig.LOG_MINING_BUFFER_LOCATION, "./target/data");
            }
            builder.withDefault(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
        }

        // In the event that the environment variables do not specify a database.pdb.name setting,
        // the test suite will then assume default CDB mode and apply the default PDB name. If
        // the environment wishes to use non-CDB mode, the database.pdb.name setting should be
        // given but without a value.
        if (!Configuration.fromSystemProperties(DATABASE_PREFIX).asMap().containsKey(PDB_NAME)) {
            builder.withDefault(OracleConnectorConfig.PDB_NAME, DATABASE);
        }

        return builder.with(OracleConnectorConfig.SERVER_NAME, SERVER_NAME)
                .with(OracleConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    /**
     * Obtain a connection using the default configuration, i.e. within the context of the
     * actual connector user that connectors and interacts with the database.
     */
    public static OracleConnection defaultConnection() {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, jdbcConfig, true);
    }

    /**
     * Returns a JdbcConfiguration for the test schema and user account.
     */
    private static JdbcConfiguration testJdbcConfig() {
        return JdbcConfiguration.copy(getDatabaseConfig(DATABASE_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, PORT)
                .with(JdbcConfiguration.USER, SCHEMA_USER)
                .with(JdbcConfiguration.PASSWORD, SCHEMA_PASS)
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    /**
     * Returns a JdbcConfiguration for the database administrator account.
     */
    private static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(getDatabaseConfig(DATABASE_ADMIN_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, PORT)
                .withDefault(JdbcConfiguration.USER, "sys as sysdba")
                .withDefault(JdbcConfiguration.PASSWORD, "top_secret")
                .withDefault(JdbcConfiguration.DATABASE, getDatabaseName())
                .build();
    }

    /**
     * Returns a configuration builder based on the test schema and user account settings.
     */
    private static Configuration.Builder testConfig() {
        JdbcConfiguration jdbcConfiguration = testJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder;
    }

    /**
     * Returns a configuration builder based on the administrator account settings.
     */
    private static Configuration.Builder adminConfig() {
        JdbcConfiguration jdbcConfiguration = adminJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder;
    }

    /**
     * Retrieves all settings provided by system properties based on the supplied {@code prefix}.
     *
     * @param prefix the key prefix to limit the settings based upon, i.e. {@code database.}.
     * @return the configuration object
     */
    private static Configuration getDatabaseConfig(String prefix) {
        // The test suite by default
        // Get properties from system and remove empty database.pdb.name
        // This will be set this way if a user wishes to test against a non-CDB environment
        Configuration config = Configuration.fromSystemProperties(prefix);
        if (config.hasKey(PDB_NAME)) {
            String pdbName = config.getString(PDB_NAME);
            if (Strings.isNullOrEmpty(pdbName)) {
                Map<String, ?> map = config.asMap();
                map.remove(PDB_NAME);
                config = Configuration.from(map);
            }
        }
        return config;
    }

    /**
     * Return a test connection that is suitable for performing test database changes in tests.
     */
    public static OracleConnection testConnection() {
        Configuration config = testConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, jdbcConfig, false);
    }

    /**
     * Return a connection that is suitable for performing test database changes that require
     * an administrator role permission.
     */
    public static OracleConnection adminConnection() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, jdbcConfig, false);
    }

    /**
     * Create an OracleConnection.
     *
     * @param config the connector configuration
     * @param jdbcConfig the JDBC configuration
     * @param autoCommit whether the connection should enforce auto-commit
     * @return the connection
     */
    private static OracleConnection createConnection(Configuration config, Configuration jdbcConfig, boolean autoCommit) {
        OracleConnection connection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader);
        try {
            connection.setAutoCommit(autoCommit);

            String pdbName = new OracleConnectorConfig(config).getPdbName();
            if (!Strings.isNullOrEmpty(pdbName)) {
                connection.setSessionToPdb(pdbName);
            }

            return connection;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create connection", e);
        }
    }

    public static void forceLogfileSwitch() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);

        try (OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader)) {
            if ((new OracleConnectorConfig(defaultConfig().build())).getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
            jdbcConnection.execute("ALTER SYSTEM SWITCH LOGFILE");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to switch logfile", e);
        }
    }

    public static int getNumberOfOnlineLogGroups() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);

        try (OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, TestHelper.class::getClassLoader)) {
            if ((new OracleConnectorConfig(defaultConfig().build())).getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
            return jdbcConnection.queryAndMap("SELECT COUNT(GROUP#) FROM V$LOG", rs -> {
                rs.next();
                return rs.getInt(1);
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get redo log groups", e);
        }
    }

    public static void forceFlushOfRedoLogsToArchiveLogs() {
        int groups = getNumberOfOnlineLogGroups();
        for (int i = 0; i < groups; ++i) {
            forceLogfileSwitch();
        }
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

    public static void dropTables(OracleConnection connection, String... tables) {
        for (String table : tables) {
            dropTable(connection, table);
        }
    }

    /**
     * Enables a given table to be streamed by Oracle.
     *
     * @param connection the oracle connection
     * @param table the table name in {@code schema.table} format.
     * @throws SQLException if an exception occurred
     */
    public static void streamTable(OracleConnection connection, String table) throws SQLException {
        connection.execute(String.format("GRANT SELECT ON %s TO %s", table, getConnectorUserName()));
        connection.execute(String.format("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", table));
    }

    /**
     * Clear the recycle bin, removing all objects from the bin and release all space associated
     * with objects in the recycle bin.  This also clears any system-generated objects that are
     * associated with a table that may have been recently dropped, such as index-organized tables.
     *
     * @param connection the oracle connection
     */
    public static void purgeRecycleBin(OracleConnection connection) {
        try {
            connection.execute("PURGE RECYCLEBIN");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to clear user recyclebin", e);
        }
    }

    /**
     * Grants the specified role to the {@link TestHelper#SCHEMA_USER} or the user configured using the
     * configuration option {@code database.user}, whichever has precedence.  If the configuration uses
     * PDB, the grant will be performed in the PDB and not the CDB database.
     *
     * @param roleName role to be granted
     * @throws RuntimeException if the role cannot be granted
     */
    public static void grantRole(String roleName) {
        final String pdbName = defaultConfig().build().getString(OracleConnectorConfig.PDB_NAME);
        final String userName = testJdbcConfig().getString(JdbcConfiguration.USER);
        try (OracleConnection connection = adminConnection()) {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            connection.execute("GRANT " + roleName + " TO " + userName);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to grant role '" + roleName + "' for user " + userName, e);
        }
    }

    /**
     * Revokes the specified role from the {@link TestHelper#SCHEMA_USER} or the user configured using
     * the configuration option {@code database.user}, whichever has precedence. If the configuration
     * uses PDB, the revoke will be performed in the PDB and not the CDB instance.
     *
     * @param roleName role to be revoked
     * @throws RuntimeException if the role cannot be revoked
     */
    public static void revokeRole(String roleName) {
        final String pdbName = defaultConfig().build().getString(OracleConnectorConfig.PDB_NAME);
        final String userName = testJdbcConfig().getString(JdbcConfiguration.USER);
        try (OracleConnection connection = adminConnection()) {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            connection.execute("REVOKE " + roleName + " FROM " + userName);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to revoke role '" + roleName + "' for user " + userName, e);
        }
    }

    public static int defaultMessageConsumerPollTimeout() {
        return 120;
    }

    public static OracleConnectorConfig.ConnectorAdapter adapter() {
        final String s = System.getProperty(OracleConnectorConfig.CONNECTOR_ADAPTER.name());
        return (s == null || s.length() == 0) ? OracleConnectorConfig.ConnectorAdapter.LOG_MINER : OracleConnectorConfig.ConnectorAdapter.parse(s);
    }

    /**
     * Drops all tables visible to user {@link #SCHEMA_USER}.
     */
    public static void dropAllTables() {
        try (OracleConnection connection = testConnection()) {
            connection.query("SELECT TABLE_NAME FROM USER_TABLES", rs -> {
                while (rs.next()) {
                    connection.execute("DROP TABLE " + rs.getString(1));
                }
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to clean database");
        }
    }

    public static List<BigInteger> getCurrentRedoLogSequences() throws SQLException {
        try (OracleConnection connection = adminConnection()) {
            return connection.queryAndMap("SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT'", rs -> {
                List<BigInteger> sequences = new ArrayList<>();
                while (rs.next()) {
                    sequences.add(new BigInteger(rs.getString(1)));
                }
                return sequences;
            });
        }
    }
}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationDefinition;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.ConnectorAdapter;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningBufferType;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.buffered.CacheProvider;
import io.debezium.connector.oracle.logminer.unbuffered.UnbufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.olr.OpenLogReplicatorStreamingChangeEventSource;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.OracleContainer;
import io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

public class TestHelper {

    private static final String PDB_NAME = "pdb.name";
    private static final String DATABASE_PREFIX = ConfigurationDefinition.DATABASE_CONFIG_PREFIX;
    private static final String DATABASE_ADMIN_PREFIX = "database.admin.";

    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

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

    public static final int INFINISPAN_HOTROD_PORT = ConfigurationProperties.DEFAULT_HOTROD_PORT;
    public static final String INFINISPAN_USER = "admin";
    public static final String INFINISPAN_PASS = "admin";
    public static final String INFINISPAN_HOST = "0.0.0.0";
    public static final String INFINISPAN_SERVER_LIST = INFINISPAN_HOST + ":" + INFINISPAN_HOTROD_PORT;

    public static final String OPENLOGREPLICATOR_SOURCE = System.getProperty("openlogreplicator.source", "ORACLE");
    public static final String OPENLOGREPLICATOR_HOST = System.getProperty("openlogreplicator.host", "localhost");
    public static final String OPENLOGREPLICATOR_PORT = System.getProperty("openlogreplicator.port", "9000");

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

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    private static final Map<String, Field> cacheMappings = new HashMap<>();

    static {
        cacheMappings.put(CacheProvider.TRANSACTIONS_CACHE_NAME, OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS);
        cacheMappings.put(CacheProvider.PROCESSED_TRANSACTIONS_CACHE_NAME, OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS);
        cacheMappings.put(CacheProvider.SCHEMA_CHANGES_CACHE_NAME, OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES);
        cacheMappings.put(CacheProvider.EVENTS_CACHE_NAME, OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS);
    }

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
                (field, value) -> builder.with(ConfigurationDefinition.DATABASE_CONFIG_PREFIX + field, value));

        if (isXStream()) {
            builder.withDefault(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout");
        }
        else if (isOpenLogReplicator()) {
            builder.withDefault(OracleConnectorConfig.OLR_SOURCE, OPENLOGREPLICATOR_SOURCE);
            builder.withDefault(OracleConnectorConfig.OLR_HOST, OPENLOGREPLICATOR_HOST);
            builder.withDefault(OracleConnectorConfig.OLR_PORT, OPENLOGREPLICATOR_PORT);
        }
        else if (isUnbufferedLogMiner()) {
            // Speeds up tests
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MIN_MS, 0);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_INCREMENT_MS, 500);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_DEFAULT_MS, 500);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MAX_MS, 1000);
        }
        else {
            // Speeds up tests
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MIN_MS, 0);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_INCREMENT_MS, 500);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_DEFAULT_MS, 500);
            builder.with(OracleConnectorConfig.LOG_MINING_SLEEP_TIME_MAX_MS, 1000);

            final Boolean readOnly = Boolean.parseBoolean(System.getProperty(OracleConnectorConfig.LOG_MINING_READ_ONLY.name()));
            if (readOnly) {
                builder.with(OracleConnectorConfig.LOG_MINING_READ_ONLY, readOnly);
            }

            final String bufferTypeName = System.getProperty(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE.name());
            final LogMiningBufferType bufferType = LogMiningBufferType.parse(bufferTypeName);
            if (bufferType.isInfinispan()) {
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferType);
                withDefaultInfinispanCacheConfigurations(bufferType, builder);
                if (!bufferType.isInfinispanEmbedded()) {
                    builder.with("log.mining.buffer." + ConfigurationProperties.SERVER_LIST, INFINISPAN_SERVER_LIST);
                    builder.with("log.mining.buffer." + ConfigurationProperties.AUTH_USERNAME, INFINISPAN_USER);
                    builder.with("log.mining.buffer." + ConfigurationProperties.AUTH_PASSWORD, INFINISPAN_PASS);
                }
            }
            else if (bufferType.isEhcache()) {
                final int cacheSize = 1024000000; // 1GB for default
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_TYPE, bufferTypeName);
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_GLOBAL_CONFIG, getEhcacheGlobalCacheConfig());
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_TRANSACTIONS_CONFIG, getEhcacheBasicCacheConfig(cacheSize));
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_PROCESSED_TRANSACTIONS_CONFIG, getEhcacheBasicCacheConfig(cacheSize));
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_SCHEMA_CHANGES_CONFIG, getEhcacheBasicCacheConfig(cacheSize));
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_EVENTS_CONFIG, getEhcacheBasicCacheConfig(cacheSize));
            }
            builder.withDefault(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
        }

        // In the event that the environment variables do not specify a database.pdb.name setting,
        // the test suite will then assume default CDB mode and apply the default PDB name. If
        // the environment wishes to use non-CDB mode, the database.pdb.name setting should be
        // given but without a value.
        if (isUsingPdb()) {
            builder.withDefault(OracleConnectorConfig.PDB_NAME, DATABASE);
        }

        return builder.with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME)
                .with(OracleConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(AsyncEmbeddedEngine.TASK_MANAGEMENT_TIMEOUT_MS, 90_000)
                .with(OracleConnectorConfig.SNAPSHOT_DATABASE_ERRORS_MAX_RETRIES, 3);
    }

    public static String getEhcacheGlobalCacheConfig() {
        return "<persistence directory=\"./target/data\"/>";
    }

    public static String getEhcacheBasicCacheConfig(int sizeBytes) {
        return "<resources>" +
                "<heap unit=\"entries\">512</heap>" +
                "<disk unit=\"B\">" + sizeBytes + "</disk>" +
                "</resources>";
    }

    /**
     * Obtain a connection using the default configuration, i.e. within the context of the
     * actual connector user that connectors and interacts with the database.
     */
    public static OracleConnection defaultConnection() {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, JdbcConfiguration.adapt(jdbcConfig), true);
    }

    /**
     * Obtain a connection using the default configuration.
     *
     * Note that the returned connection will automatically switch to the container database root
     * if {@code switchToRoot} is specified as {@code true}.  If the connection is not configured
     * to use pluggable databases or pluggable databases are not enabled, the argument has no
     * effect on the returned connection.
     */
    public static OracleConnection defaultConnection(boolean switchToRoot) {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        final OracleConnection connection = createConnection(config, JdbcConfiguration.adapt(jdbcConfig), true);
        if (switchToRoot && isUsingPdb()) {
            connection.resetSessionToCdb();
        }
        return connection;
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
    public static Configuration.Builder testConfig() {
        JdbcConfiguration jdbcConfiguration = testJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(ConfigurationDefinition.DATABASE_CONFIG_PREFIX + field, value));

        builder.with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME);
        return builder;
    }

    /**
     * Returns a configuration builder based on the administrator account settings.
     */
    private static Configuration.Builder adminConfig() {
        JdbcConfiguration jdbcConfiguration = adminJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(ConfigurationDefinition.DATABASE_CONFIG_PREFIX + field, value));

        builder.with(CommonConnectorConfig.TOPIC_PREFIX, SERVER_NAME);
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
        return createConnection(config, JdbcConfiguration.adapt(jdbcConfig), false);
    }

    /**
     * Return a test connection that is suitable for performing test database changes in tests.
     */
    public static OracleConnection testConnection(Configuration config) {

        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, JdbcConfiguration.adapt(jdbcConfig), false);
    }

    /**
     * Return a connection that is suitable for performing test database changes that require
     * an administrator role permission.
     *
     * Additionally, the connection returned will be associated to the configured pluggable
     * database if one is configured otherwise the root database.
     */
    public static OracleConnection adminConnection() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        return createConnection(config, JdbcConfiguration.adapt(jdbcConfig), false);
    }

    /**
     * Return a connection that is suitable for performing test database changes that require
     * an administrator role permission.
     *
     * Note that the returned connection will automatically switch to the container database root
     * if {@code switchToRoot} is specified as {@code true}.  If the connection is not configured
     * to use pluggable databases or pluggable databases are not enabled, the argument has no
     * effect on the returned connection.
     */
    public static OracleConnection adminConnection(boolean switchToRoot) {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset(DATABASE_PREFIX, true);
        final OracleConnection connection = createConnection(config, JdbcConfiguration.adapt(jdbcConfig), false);
        if (switchToRoot && isUsingPdb()) {
            connection.resetSessionToCdb();
        }
        return connection;
    }

    /**
     * Create an OracleConnection.
     *
     * @param config the connector configuration
     * @param jdbcConfig the JDBC configuration
     * @param autoCommit whether the connection should enforce auto-commit
     * @return the connection
     */
    private static OracleConnection createConnection(Configuration config, JdbcConfiguration jdbcConfig, boolean autoCommit) {
        OracleConnection connection = new OracleConnection(jdbcConfig);
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

        try (OracleConnection jdbcConnection = new OracleConnection(JdbcConfiguration.adapt(jdbcConfig))) {
            if (!Strings.isNullOrEmpty((new OracleConnectorConfig(defaultConfig().build())).getPdbName())) {
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

        try (OracleConnection jdbcConnection = new OracleConnection(JdbcConfiguration.adapt(jdbcConfig))) {
            if (!Strings.isNullOrEmpty((new OracleConnectorConfig(defaultConfig().build())).getPdbName())) {
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

    public static void dropSequence(OracleConnection connection, String sequence) {
        try {
            connection.execute("DROP SEQUENCE " + sequence);
        }
        catch (SQLException e) {
            // ORA-02289 - sequence does not exist
            // Since Oracle does not support "IF EXISTS", only throw exceptions that aren't ORA-02289
            if (!e.getMessage().contains("ORA-02289") || 2289 != e.getErrorCode()) {
                throw new RuntimeException(e);
            }
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
        grantRole(roleName, null, testJdbcConfig().getString(JdbcConfiguration.USER));
    }

    /**
     * Grants the specified roles to the {@link TestHelper#SCHEMA_USER} or the user configured using the
     * configuration option {@code database.user}, which has precedence, on the specified object.  If
     * the configuration uses PDB, the grant will be performed int he PDB and not the CDB database.
     *
     * @param roleName role to be granted
     * @param objectName the object to grant the role against
     * @param userName the user to whom the grant should be applied
     * @throws RuntimeException if the role cannot be granted
     */
    public static void grantRole(String roleName, String objectName, String userName) {
        final String pdbName = defaultConfig().build().getString(OracleConnectorConfig.PDB_NAME);
        try (OracleConnection connection = adminConnection()) {
            if (pdbName != null) {
                connection.setSessionToPdb(pdbName);
            }
            final StringBuilder sql = new StringBuilder("GRANT ").append(roleName);
            if (!Strings.isNullOrEmpty(objectName)) {
                sql.append(" ON ").append(objectName);
            }
            sql.append(" TO ").append(userName);
            System.out.println(sql.toString());
            connection.execute(sql.toString());
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
        final String messageConsumerPollTimeout = System.getProperty("test.message.consumer.poll.timeout");
        if (!Strings.isNullOrEmpty(messageConsumerPollTimeout)) {
            try {
                return Integer.parseInt(messageConsumerPollTimeout);
            }
            catch (Exception e) {
                LOGGER.warn("The provided 'test.message.consumer.poll.timeout' is invalid, using defaults", e);
            }
        }

        // Speeds up tests for LogMiner and OLR
        return isXStream() ? 120 : 20;
    }

    public static ConnectorAdapter adapter() {
        final String s = System.getProperty(OracleConnectorConfig.CONNECTOR_ADAPTER.name());
        return (s == null || s.length() == 0) ? ConnectorAdapter.LOG_MINER : ConnectorAdapter.parse(s);
    }

    public static boolean isAnyLogMiner() {
        return isBufferedLogMiner() || isUnbufferedLogMiner();
    }

    public static boolean isBufferedLogMiner() {
        return ConnectorAdapter.LOG_MINER.equals(adapter());
    }

    public static boolean isUnbufferedLogMiner() {
        return ConnectorAdapter.LOG_MINER_UNBUFFERED.equals(adapter());
    }

    public static boolean isXStream() {
        return ConnectorAdapter.XSTREAM.equals(adapter());
    }

    public static boolean isOpenLogReplicator() {
        return ConnectorAdapter.OLR.equals(adapter());
    }

    public static LogMiningStrategy logMiningStrategy() {
        if (isAnyLogMiner()) {
            // This won't catch all use cases where the user overrides the default configuration in the test
            // itself but generally this should be satisfactory for marker annotations based on static or
            // CLI provided configurations.
            Configuration configuration = TestHelper.defaultConfig().build();
            return LogMiningStrategy.parse(configuration.getString(OracleConnectorConfig.LOG_MINING_STRATEGY));
        }
        return null;
    }

    /**
     * Drops all tables visible to user {@link #SCHEMA_USER}.
     */
    public static void dropAllTables() {
        try (OracleConnection connection = testConnection()) {
            connection.query("SELECT TABLE_NAME FROM USER_TABLES", rs -> {
                while (rs.next()) {
                    // Oracle normally stores tables in upper case; however, if a table is created using
                    // special characters, it must be quoted and therefore is treated as case-sensitive,
                    // which will require quotes. This checks this specific use case and quotes the name
                    // of the table if necessary.
                    String tableName = rs.getString(1);
                    if (isQuoteRequired(tableName)) {
                        tableName = "\"" + tableName + "\"";
                    }
                    dropTable(connection, String.format("%s.%s", SCHEMA_USER, tableName));
                }
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to clean database", e);
        }
    }

    public static boolean isQuoteRequired(String tableName) {
        if (!Strings.isNullOrBlank(tableName)) {
            // Make sure table isn't already quoted
            if (!tableName.startsWith("\"") && !tableName.endsWith("\"")) {
                for (int i = 0; i < tableName.length(); i++) {
                    final char c = tableName.charAt(i);
                    // If we detect any lower case character or non letter/digit, name must be quoted
                    if (Character.isLowerCase(c) || !Character.isLetterOrDigit(c)) {
                        return true;
                    }
                }
            }
        }
        return false;
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

    public static String getDefaultInfinispanEmbeddedCacheConfig(String cacheName) {
        return new org.infinispan.configuration.cache.ConfigurationBuilder()
                .persistence()
                .passivation(false)
                .addSoftIndexFileStore()
                .segmented(true)
                .preload(true)
                .shared(false)
                .ignoreModifications(false)
                .dataLocation("./target/data")
                .indexLocation("./target/data")
                .build()
                .toStringConfiguration(cacheName);
    }

    public static String getDefaultInfinispanRemoteCacheConfig(String cacheName) {
        return "<distributed-cache name=\"" + cacheName + "\" statistics=\"true\">\n" +
                "\t<encoding media-type=\"application/x-protostream\"/>\n" +
                "\t<persistence passivation=\"false\">\n" +
                "\t\t<file-store read-only=\"false\" preload=\"true\" shared=\"false\" segmented=\"true\"/>\n" +
                "\t</persistence>\n" +
                "</distributed-cache>";
    }

    public static Configuration.Builder withDefaultInfinispanCacheConfigurations(LogMiningBufferType bufferType, Configuration.Builder builder) {
        for (Map.Entry<String, Field> cacheMapping : cacheMappings.entrySet()) {
            final Field field = cacheMapping.getValue();
            final String cacheName = cacheMapping.getKey();

            final String config = bufferType.isInfinispanEmbedded()
                    ? getDefaultInfinispanEmbeddedCacheConfig(cacheName)
                    : getDefaultInfinispanRemoteCacheConfig(cacheName);

            builder.with(field, config);
        }

        if (bufferType.isInfinispanEmbedded()) {
            builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL,
                    getDefaultInfinispanEmbeddedCacheConfig("global"));
        }

        return builder;
    }

    /**
     * Simulate {@link Thread#sleep(long)} by using {@link Awaitility} instead.
     *
     * @param duration the duration to sleep (wait)
     * @param units the unit of time
     * @throws Exception if the wait/sleep failed
     */
    public static void sleep(long duration, TimeUnit units) throws Exception {
        // While we wait 1 additional unit more than the requested sleep timer, the poll delay is offset
        // by exactly the sleep time and the condition always return true and so the extended atMost
        // value is irrelevant and only used to satisfy Awaitility's need for atMost > pollDelay.
        Awaitility.await().atMost(duration + 1, units).pollDelay(duration, units).until(() -> true);
    }

    /**
     * Get a valid {@link OracleConnectorConfig#URL} string.
     */
    public static String getOracleConnectionUrlDescriptor() {
        final StringBuilder url = new StringBuilder();
        url.append("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=");
        url.append(HOST);
        url.append(")(PORT=").append(PORT).append("))");
        url.append("(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=").append(getDatabaseName()).append(")))");
        return url.toString();
    }

    /**
     * Returns whether the connection is using a pluggable database configuration.
     */
    public static boolean isUsingPdb() {
        final Map<String, String> properties = Configuration.fromSystemProperties(DATABASE_PREFIX).asMap();
        if (properties.containsKey(PDB_NAME)) {
            // if the property is specified and is not null/empty, we are using PDB mode.
            return !Strings.isNullOrEmpty(properties.get(PDB_NAME));
        }
        // if the property is not specified, we default to using PDB mode.
        return Strings.isNullOrEmpty(properties.get(PDB_NAME));
    }

    /**
     * Returns the connector adapter from the provided configuration.
     *
     * @param config the connector configuration, must not be {@code null}
     * @return the connector adapter being used.
     */
    public static ConnectorAdapter getAdapter(Configuration config) {
        return ConnectorAdapter.parse(config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER));
    }

    /**
     * Returns the current system change number in the database.
     *
     * @return the current system change number, never {@code null}
     * @throws SQLException if a database error occurred
     */
    public static Scn getCurrentScn() throws SQLException {
        try (OracleConnection admin = new OracleConnection(adminJdbcConfig(), false)) {
            // Force the connection to the CDB$ROOT if we're operating w/a PDB
            if (isUsingPdb()) {
                admin.resetSessionToCdb();
            }
            return admin.getCurrentScn();
        }
    }

    // Below are test helper methods for integration tests using the Testcointainers based OracleContainer instance:

    private static Configuration getTestConnectionConfiguration(ConnectorConfiguration config) {
        var connectionConfiguration = Configuration.from(config.asProperties()).subset(ConfigurationDefinition.DATABASE_CONFIG_PREFIX, true);
        var dbName = Strings.isNullOrEmpty(connectionConfiguration.getString(PDB_NAME))
                ? connectionConfiguration.getString(JdbcConfiguration.DATABASE)
                : connectionConfiguration.getString(PDB_NAME);
        return connectionConfiguration.edit()
                .with(JdbcConfiguration.HOSTNAME.name(), "localhost")
                .with(JdbcConfiguration.PORT, TestInfrastructureHelper.getOracleContainer().getMappedPort(OracleContainer.ORACLE_PORT))
                .with(JdbcConfiguration.DATABASE, dbName)
                .build();
    }

    private static void patchConnectorConfigurationForContainer(ConnectorConfiguration connectorConfiguration, OracleContainer oracleContainer) {
        var oracleImageName = oracleContainer.getDockerImageName();
        if (!oracleImageName.startsWith(OracleContainer.DEFAULT_IMAGE_NAME.getUnversionedPart())) {
            return;
        }
        String imageTag = "latest";
        String imageTagSuffix = "";
        if (oracleImageName.contains(":")) {
            imageTag = oracleImageName.split(":")[1];
        }
        if (imageTag.contains("-")) {
            imageTagSuffix = imageTag.substring(imageTag.lastIndexOf("-") + 1);
        }
        String pdbName = connectorConfiguration.asProperties().getProperty(OracleConnectorConfig.PDB_NAME.name());
        if (!imageTag.contains("-") || "xs".equals(imageTagSuffix)) {
            if (!Strings.isNullOrEmpty(pdbName)) {
                connectorConfiguration.with(OracleConnectorConfig.DATABASE_NAME.name(), pdbName);
            }
        }
        else if ("noncdb".equals(imageTagSuffix)) {
            if (!Strings.isNullOrEmpty(pdbName)) {
                connectorConfiguration.remove(OracleConnectorConfig.PDB_NAME.name());
            }
        }
        else {
            throw new RuntimeException("Invalid or unknown image tag '" + imageTagSuffix + "' for Oracle container image: " + oracleImageName);
        }
    }

    public static long getUndoRetentionSeconds() throws SQLException {
        try (OracleConnection admin = adminConnection(false)) {
            return admin.queryAndMap(
                    "SELECT VALUE from V$PARAMETER WHERE NAME = 'undo_retention'",
                    admin.singleResultMapper(rs -> rs.getLong(1), "Failed to get undo retention parameter"));
        }
    }

    public static LogInterceptor getEventProcessorLogInterceptor() {
        return switch (adapter()) {
            case LOG_MINER -> new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            case LOG_MINER_UNBUFFERED -> new LogInterceptor(UnbufferedLogMinerStreamingChangeEventSource.class);
            case XSTREAM -> new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");
            case OLR -> new LogInterceptor(OpenLogReplicatorStreamingChangeEventSource.class);
        };
    }

    public static LogInterceptor getAbstractEventProcessorLogInterceptor() {
        return switch (adapter()) {
            case LOG_MINER, LOG_MINER_UNBUFFERED -> new LogInterceptor(AbstractLogMinerStreamingChangeEventSource.class);
            case XSTREAM -> new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");
            case OLR -> new LogInterceptor(OpenLogReplicatorStreamingChangeEventSource.class);
        };
    }

    public static LogInterceptor getEventCommitHandler() {
        return switch (adapter()) {
            case LOG_MINER, LOG_MINER_UNBUFFERED -> new LogInterceptor(TransactionCommitConsumer.class);
            case XSTREAM -> new LogInterceptor("io.debezium.connector.oracle.xstream.LcrEventHandler");
            case OLR -> new LogInterceptor(OpenLogReplicatorStreamingChangeEventSource.class);
        };
    }

}

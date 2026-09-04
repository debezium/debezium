/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import static io.debezium.connector.oracle.jdbc.OracleJdbcConfiguration.SECONDARY_PREFIX;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
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
import io.debezium.config.ConfigurationNames;
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
import io.debezium.util.DelayStrategy;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

public class TestHelper {

    private static final String PDB_NAME = "pdb.name";
    private static final String DATABASE_PREFIX = ConfigurationNames.DATABASE_CONFIG_PREFIX;
    private static final String DATABASE_ADMIN_PREFIX = "database.admin.";

    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    public static final String CONNECTOR_NAME = "oracle";
    public static final String SERVER_NAME = "server1";
    public static final String HOST = "localhost";
    public static final int PORT = 1521;

    public static final int INFINISPAN_HOTROD_PORT = ConfigurationProperties.DEFAULT_HOTROD_PORT;
    public static final String INFINISPAN_USER = "admin";
    public static final String INFINISPAN_PASS = "admin";
    public static final String INFINISPAN_HOST = "0.0.0.0";
    public static final String INFINISPAN_SERVER_LIST = INFINISPAN_HOST + ":" + INFINISPAN_HOTROD_PORT;

    public static final String OPENLOGREPLICATOR_SOURCE = System.getProperty("openlogreplicator.source", "ORACLE");
    public static final String OPENLOGREPLICATOR_HOST = System.getProperty("openlogreplicator.host", "localhost");
    public static final String OPENLOGREPLICATOR_PORT = System.getProperty("openlogreplicator.port", "9000");

    // Maximum SCN value from Oracle 19+
    public static final Scn SCN_MAX = Scn.valueOf("18446744073709551615");

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
        cacheMappings.put(CacheProvider.ROLLBACKS_CACHE_NAME, OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACKS);
    }

    /**
     * The database topologies the test suite can run against.
     * <p>
     * Each mode supplies the default connection values for that topology; any explicitly provided
     * {@code -Ddatabase.*}, {@code -Ddatabase.admin.*}, or {@code -Dschema.*} property overrides
     * the mode's default.
     */
    public enum DatabaseMode {
        /** A container database, capturing from a pluggable database; the historical default. */
        CDB_PDB("cdb", "c##dbzuser", "dbz", "sys as sysdba", "top_secret", "ORCLCDB", "ORCLPDB1", "dbz", true),

        /** A non-container database topology; the database name matches the single database instance. */
        NON_CDB("non-cdb", "c##dbzuser", "dbz", "sys as sysdba", "top_secret", "ORCLCDB", "ORCLCDB", "dbz", false),

        /** An Oracle Autonomous Database, e.g. the adb-free container. */
        AUTONOMOUS("adb", "GGADMIN", "Welcome_1234", "ADMIN", "Welcome_1234", "MYATP", "MYATP", "Dbz_Adb_Tests_2026", false);

        private final String label;
        private final String connectorUser;
        private final String connectorPassword;
        private final String adminUser;
        private final String adminPassword;
        private final String connectorDatabase;
        private final String databaseName;
        private final String schemaPassword;
        private final boolean usesPdb;

        DatabaseMode(String label, String connectorUser, String connectorPassword, String adminUser, String adminPassword,
                     String connectorDatabase, String databaseName, String schemaPassword, boolean usesPdb) {
            this.label = label;
            this.connectorUser = connectorUser;
            this.connectorPassword = connectorPassword;
            this.adminUser = adminUser;
            this.adminPassword = adminPassword;
            this.connectorDatabase = connectorDatabase;
            this.databaseName = databaseName;
            this.schemaPassword = schemaPassword;
            this.usesPdb = usesPdb;
        }

        public String getConnectorUser() {
            return connectorUser;
        }

        public String getConnectorPassword() {
            return connectorPassword;
        }

        public String getAdminUser() {
            return adminUser;
        }

        public String getAdminPassword() {
            return adminPassword;
        }

        public String getConnectorDatabase() {
            return connectorDatabase;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public String getSchemaPassword() {
            return schemaPassword;
        }

        public boolean isUsesPdb() {
            return usesPdb;
        }

        static DatabaseMode parse(String value) {
            for (DatabaseMode mode : values()) {
                if (mode.label.equalsIgnoreCase(value) || mode.name().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException(String.format(
                    "Unknown test.database.mode '%s', expected one of: cdb, non-cdb, adb", value));
        }
    }

    /**
     * Resolves the database topology the test suite runs against.
     * <p>
     * The {@code test.database.mode} system property ({@code cdb}, {@code non-cdb}, or {@code adb}) selects
     * the mode explicitly. When it is absent, the mode is inferred from the legacy signals for backward
     * compatibility:
     * <ul>
     *     <li>{@code -Doracle.adb=true} selects {@link DatabaseMode#AUTONOMOUS}</li>
     *     <li>{@code -Ddatabase.pdb.name} present but empty selects {@link DatabaseMode#NON_CDB}</li>
     *     <li>Otherwise uses {@link DatabaseMode#CDB_PDB}</li>
     * </ul>
     */
    public static DatabaseMode getDatabaseMode() {
        final String mode = System.getProperty("test.database.mode");
        if (!Strings.isNullOrEmpty(mode)) {
            return DatabaseMode.parse(mode);
        }

        if (Boolean.parseBoolean(System.getProperty("oracle.adb", "false"))) {
            return DatabaseMode.AUTONOMOUS;
        }

        final Map<String, String> properties = Configuration.fromSystemProperties(DATABASE_PREFIX).asMap();
        if (properties.containsKey(PDB_NAME) && Strings.isNullOrEmpty(properties.get(PDB_NAME))) {
            return DatabaseMode.NON_CDB;
        }

        return DatabaseMode.CDB_PDB;
    }

    /**
     * Get the name of the connector user.
     */
    public static String getConnectorUserName() {
        final String userName = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.USER.name());
        return Strings.isNullOrEmpty(userName) ? getDatabaseMode().getConnectorUser() : userName;
    }

    /**
     * Get the password of the connector user.
     */
    private static String getConnectorUserPassword() {
        final String password = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.PASSWORD.name());
        return Strings.isNullOrEmpty(password) ? getDatabaseMode().getConnectorPassword() : password;
    }

    /**
     * Get the database name, defaulting to the value supplied by the {@link DatabaseMode}.
     */
    public static String getDatabaseName() {
        final String databaseName = getDatabaseConfig(DATABASE_PREFIX).getString(JdbcConfiguration.DATABASE);
        return Strings.isNullOrEmpty(databaseName) ? getDatabaseMode().getDatabaseName() : databaseName;
    }

    /**
     * The test schema username is a suite-wide invariant. It appears as a literal in table include lists
     * and topic name assertions throughout the tests and is deliberately not overridable.
     */
    public static String getSchemaUserName() {
        return "DEBEZIUM";
    }

    public static String getSchemaPassword() {
        return System.getProperty("schema.password", getDatabaseMode().getSchemaPassword());
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
                .withDefault(JdbcConfiguration.DATABASE, getDatabaseMode().getConnectorDatabase())
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
                (field, value) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + field, value));

        // Allows specifying -Dcapture.mode from CLI
        if (!Strings.isNullOrEmpty(System.getProperty("capture.mode"))) {
            builder.with(OracleConnectorConfig.CAPTURE_MODE, System.getProperty("capture.mode"));
        }

        // Allows specifying -Dsecondary.* properties from CLI
        Configuration.fromSystemProperties(SECONDARY_PREFIX)
                .forEach((field, value) -> builder.with(SECONDARY_PREFIX + field, value));

        if (isXStream()) {
            builder.withDefault(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout");
        }
        else if (isOpenLogReplicator()) {
            builder.withDefault(OracleConnectorConfig.OLR_SOURCE, OPENLOGREPLICATOR_SOURCE);
            builder.withDefault(OracleConnectorConfig.OLR_HOST, OPENLOGREPLICATOR_HOST);
            builder.withDefault(OracleConnectorConfig.OLR_PORT, OPENLOGREPLICATOR_PORT);
        }
        else if (isBufferedLogMiner()) {
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
                builder.with(OracleConnectorConfig.LOG_MINING_BUFFER_EHCACHE_ROLLBACKS_CONFIG, getEhcacheBasicCacheConfig(cacheSize));
            }
            builder.withDefault(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, true);
        }

        // In the event that the environment variables do not specify a database.pdb.name setting,
        // the test suite will then assume default CDB mode and apply the default PDB name. If
        // the environment wishes to use non-CDB mode, the database.pdb.name setting should be
        // given but without a value.
        if (isUsingPdb()) {
            builder.withDefault(OracleConnectorConfig.PDB_NAME, getDatabaseMode().getDatabaseName());
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
                .with(JdbcConfiguration.USER, getSchemaUserName())
                .with(JdbcConfiguration.PASSWORD, getSchemaPassword())
                .withDefault(JdbcConfiguration.DATABASE, getDatabaseName())
                .build();
    }

    /**
     * Returns a JdbcConfiguration for the database administrator account.
     */
    private static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(getDatabaseConfig(DATABASE_ADMIN_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, PORT)
                .withDefault(JdbcConfiguration.USER, getDatabaseMode().getAdminUser())
                .withDefault(JdbcConfiguration.PASSWORD, getDatabaseMode().getAdminPassword())
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
                (field, value) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + field, value));

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
                (field, value) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + field, value));

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
        // Setting this to true at least keeps existing behavior, expecting tests to set this to false
        // as needed since this connection is not used in the connector but as part of the test, to
        // perform required database SQL operations.
        OracleConnection connection = new OracleConnection(jdbcConfig, true);
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

        try (OracleConnection jdbcConnection = new OracleConnection(JdbcConfiguration.adapt(jdbcConfig), true)) {
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

        try (OracleConnection jdbcConnection = new OracleConnection(JdbcConfiguration.adapt(jdbcConfig), true)) {
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

    /**
     * Returns whether the test suite targets an Oracle Autonomous Database, see {@link #getDatabaseMode()}.
     * <p>
     * Maven {@code oracle-adb} profile selects this mode automatically; IDE runs should pass the
     * {@code -Doracle.adb=true} argument or {@code -Dtest.database.mode=adb}.
     * <p>
     * The mode is not derived from the database itself, as the connection defaults depend on result of
     * this method, and doing so would create a cyclic dependency.
     */
    public static boolean isAutonomousDatabase() {
        return getDatabaseMode() == DatabaseMode.AUTONOMOUS;
    }

    /**
     * Forces pending changes to become visible to the streaming engine.
     * <p>
     * On an Autonomous Database, changes only become visible to LogMiner once the current online redo log
     * has been archived, and no privileged path exists to trigger archive. The ADB service blocks any type
     * of {@code ALTER SYSTEM} through lockdown, and the ADB Free container denies all OS-authenticated
     * administrative logons while the database is open, shipping without a password file or {@code orapwd}
     * utility. This is done to emulate the Oracle Cloud Infrastructure ADB deployment.
     * <p>
     * Instead, the archive is provoked without any special privileges by generating enough throwaway redo
     * through the test schema connection to fill the current online redo log, which causes the database
     * to switch and archive it. The redo is rolled back, so there is no visible-changes, just a rollback
     * transaction that the connector ignores. The generated volume is bounded and stops as soon as an
     * archive log is observed.
     * <p>
     * If this were used on a non-Autonomous database, this method is a no-op, as changes in the online redo
     * logs are immediately visible to Debezium.
     */
    public static void forceStreamingVisibility() {
        if (!isAutonomousDatabase()) {
            return;
        }

        try {
            generateRedoUntilLogArchived();
        }
        catch (Exception e) {
            LOGGER.warn("Unable to provoke a redo log archive; relying on the database's natural log switch cadence", e);
        }
    }

    private static void generateRedoUntilLogArchived() throws SQLException {
        // Roughly 6MB of row data per chunk; with undo the redo generated per chunk is larger.
        // The adb-free container uses 20MB online redo logs, so a handful of chunks suffices.
        final int maxChunks = 10;
        final String redoChunkBlock = "DECLARE l_data VARCHAR2(4000) := RPAD('x', 4000, 'x');" +
                " BEGIN" +
                "   FOR i IN 1..1500 LOOP" +
                "     INSERT INTO " + getSchemaUserName() + ".adb_redo_pump (data) VALUES (l_data);" +
                "   END LOOP;" +
                "   ROLLBACK;" +
                " END;";

        try (OracleConnection admin = defaultConnection(); OracleConnection schema = testConnection()) {
            final long startSequence = maxArchivedLogSequence(admin);

            try {
                schema.execute("CREATE TABLE adb_redo_pump (data varchar2(4000))");
            }
            catch (SQLException e) {
                // ORA-00955 - the pump table already exists
                if (e.getErrorCode() != 955) {
                    throw e;
                }
            }

            for (int chunk = 0; chunk < maxChunks; chunk++) {
                schema.execute(redoChunkBlock);
                if (maxArchivedLogSequence(admin) > startSequence) {
                    return;
                }
            }

            // The redo generated may still be in flight to the archiver; give it a moment
            final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
            while (System.currentTimeMillis() < deadline) {
                if (maxArchivedLogSequence(admin) > startSequence) {
                    return;
                }
                Awaitility.await().pollDelay(Duration.ofMillis(500)).timeout(Duration.ofSeconds(1)).until(() -> true);
            }

            LOGGER.warn("No newly archived log was observed after generating redo; streaming visibility is not guaranteed");
        }
    }

    private static long maxArchivedLogSequence(OracleConnection connection) throws SQLException {
        return connection.queryAndMap("SELECT NVL(MAX(SEQUENCE#), 0) FROM V$ARCHIVED_LOG", rs -> {
            rs.next();
            return rs.getLong(1);
        });
    }

    public static void dropTable(OracleConnection connection, String table) {
        final DelayStrategy strategy = DelayStrategy.exponential(Duration.ofSeconds(1), Duration.ofSeconds(30));
        final int maxAttempts = 10;

        int attempt = 0;
        while (attempt < maxAttempts) {
            try {
                connection.execute("DROP TABLE " + table);
                return;
            }
            catch (SQLException e) {
                // ORA-00054 - Resource is busy
                if (e.getErrorCode() == 54 || e.getMessage().startsWith("ORA-00054")) {
                    attempt++;
                    if (attempt < maxAttempts) {
                        LOGGER.warn("ORA-00054 table '{}' is busy, drop table will be retried ({} / {}).", table, attempt + 1, maxAttempts);
                        strategy.sleepWhen(true);
                        continue;
                    }
                    LOGGER.error("ORA-00054 table '{}' is busy, drop table failed.", table);
                }
                // ORA-00942 - table or view does not exist
                else if (e.getErrorCode() == 942 || e.getMessage().startsWith("ORA-00942")) {
                    LOGGER.warn("ORA-00942 table '{}' does not exist, drop table skipped.", table);
                    return;
                }

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
        try {
            connection.execute(String.format("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", table));
        }
        catch (SQLException e) {
            // Supplemental logging already exists, we can ignore
            if (e.getErrorCode() != 32588) {
                throw e;
            }
        }
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
     * Grants the specified role to the schema username or the user configured using the
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
     * Grants the specified roles to the schema username or the user configured using the
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
     * Revokes the specified role from the schema username or the user configured using
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
     * Drops all tables visible to schema username.
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
                    dropTable(connection, String.format("%s.%s", getSchemaUserName(), tableName));
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
        // if the property is not specified, the database mode decides.
        return getDatabaseMode().isUsesPdb();
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
        try (OracleConnection admin = new OracleConnection(adminJdbcConfig(), true)) {
            // Force the connection to the CDB$ROOT if we're operating w/a PDB
            if (isUsingPdb()) {
                admin.resetSessionToCdb();
            }
            return admin.getCurrentScn();
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

    public static void enableGoldenGateReplication() throws SQLException {
        try (OracleConnection admin = adminConnection(true)) {
            admin.execute("ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=BOTH");
        }
    }

    public static void disableGoldenGateReplication() throws SQLException {
        try (OracleConnection admin = adminConnection(true)) {
            admin.execute("ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=BOTH");
        }
    }
}

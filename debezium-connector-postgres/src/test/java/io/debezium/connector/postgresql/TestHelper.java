/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SecureConnectionMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Throwables;

/**
 * A utility for integration test cases to connect the PostgreSQL server running in the Docker container created by this module's
 * build.
 *
 * @author Horia Chiorean
 */
public final class TestHelper {

    public static final String CONNECTION_TEST = "Debezium Test";
    public static final String TEST_SERVER = "test_server";
    protected static final String TEST_DATABASE = "postgres";
    protected static final String PK_FIELD = "pk";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    /**
     * Key for schema parameter used to store DECIMAL/NUMERIC columns' precision.
     */
    static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    /**
     * Key for schema parameter used to store a source column's name.
     */
    static final String COLUMN_NAME_PARAMETER_KEY = "__debezium.source.column.name";

    private TestHelper() {
    }

    /**
     * Obtain a replication connection instance for the given slot name.
     *
     * @param slotName the name of the logical decoding slot
     * @param dropOnClose true if the slot should be dropped upon close
     * @param config customized connector configuration
     * @return the PostgresConnection instance; never null
     * @throws SQLException if there is a problem obtaining a replication connection
     */
    public static ReplicationConnection createForReplication(String slotName, boolean dropOnClose,
                                                             PostgresConnectorConfig config)
            throws SQLException {
        final PostgresConnectorConfig.LogicalDecoder plugin = decoderPlugin();
        return ReplicationConnection.builder(config)
                .withPlugin(plugin)
                .withSlot(slotName)
                .withTypeRegistry(getTypeRegistry())
                .dropSlotOnClose(dropOnClose)
                .statusUpdateInterval(Duration.ofSeconds(10))
                .withSchema(getSchema(config))
                .jdbcMetadataConnection(TestHelper.createWithTypeRegistry())
                .build();
    }

    /**
     * Obtain a replication connection instance for the given slot name.
     *
     * @param slotName the name of the logical decoding slot
     * @param dropOnClose true if the slot should be dropped upon close
     * @return the PostgresConnection instance; never null
     * @throws SQLException if there is a problem obtaining a replication connection
     */
    public static ReplicationConnection createForReplication(String slotName, boolean dropOnClose) throws SQLException {
        return createForReplication(slotName, dropOnClose, new PostgresConnectorConfig(defaultConfig().build()));
    }

    /**
     * @return the decoder plugin used for testing and configured by system property
     */
    public static PostgresConnectorConfig.LogicalDecoder decoderPlugin() {
        final String s = System.getProperty(PostgresConnectorConfig.PLUGIN_NAME.name());
        return (s == null || s.length() == 0) ? PostgresConnectorConfig.LogicalDecoder.DECODERBUFS : PostgresConnectorConfig.LogicalDecoder.parse(s);
    }

    /**
     * Obtain a default DB connection.
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection create() {
        return new PostgresConnection(defaultJdbcConfig(), CONNECTION_TEST);
    }

    /**
     * Obtain a default DB connection.
     *
     * @param jdbcConfiguration jdbc configuration to use
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection create(JdbcConfiguration jdbcConfiguration) {
        return new PostgresConnection(jdbcConfiguration, CONNECTION_TEST);
    }

    /**
     * Obtain a DB connection providing type registry.
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection createWithTypeRegistry() {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(defaultConfig().build());

        return new PostgresConnection(
                config.getJdbcConfig(),
                getPostgresValueConverterBuilder(config),
                CONNECTION_TEST);
    }

    /**
     * Obtain a DB connection with a custom application name.
     *
     * @param appName the name of the application used for PostgreSQL diagnostics
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection create(String appName) {
        return new PostgresConnection(JdbcConfiguration.adapt(defaultJdbcConfig().edit().with("ApplicationName", appName).build()), CONNECTION_TEST);
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the connection
     *
     * @param statement A SQL statement
     * @param furtherStatements Further SQL statement(s)
     */
    public static void execute(String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            for (String further : furtherStatements) {
                statement = statement + further;
            }
        }

        try (PostgresConnection connection = create()) {
            connection.setAutoCommit(false);
            connection.executeWithoutCommitting(statement);
            Connection jdbcConn = connection.connection();
            if (!statement.endsWith("ROLLBACK;")) {
                jdbcConn.commit();
            }
            else {
                jdbcConn.rollback();
            }
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes a JDBC statement using the default jdbc config without committing the connection
     *
     * @param statement A SQL statement
     * @param furtherStatements Further SQL statement(s)
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection executeWithoutCommit(String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            for (String further : furtherStatements) {
                statement = statement + further;
            }
        }

        try {
            PostgresConnection connection = create();
            connection.setAutoCommit(false);
            connection.executeWithoutCommitting(statement);
            Connection jdbcConn = connection.connection();
            if (statement.endsWith("ROLLBACK;")) {
                jdbcConn.rollback();
            }
            return connection;
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Drops all the public non system schemas from the DB.
     *
     * @throws SQLException if anything fails.
     */
    public static void dropAllSchemas() throws SQLException {
        String lineSeparator = System.lineSeparator();
        Set<String> schemaNames = schemaNames();
        if (!schemaNames.contains(PostgresSchema.PUBLIC_SCHEMA_NAME)) {
            schemaNames.add(PostgresSchema.PUBLIC_SCHEMA_NAME);
        }
        String dropStmts = schemaNames.stream()
                .map(schema -> "\"" + schema.replaceAll("\"", "\"\"") + "\"")
                .map(schema -> "DROP SCHEMA IF EXISTS " + schema + " CASCADE;")
                .collect(Collectors.joining(lineSeparator));
        TestHelper.execute(dropStmts);
        try {
            TestHelper.executeDDL("init_database.ddl");
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to initialize database", e);
        }
    }

    public static TypeRegistry getTypeRegistry() {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(defaultConfig().build());
        try (PostgresConnection connection = new PostgresConnection(config.getJdbcConfig(), getPostgresValueConverterBuilder(config), CONNECTION_TEST)) {
            return connection.getTypeRegistry();
        }
    }

    public static PostgresDefaultValueConverter getDefaultValueConverter() {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(defaultConfig().build());
        try (PostgresConnection connection = new PostgresConnection(config.getJdbcConfig(), getPostgresValueConverterBuilder(config), CONNECTION_TEST)) {
            return connection.getDefaultValueConverter();
        }
    }

    public static Charset getDatabaseCharset() {
        final PostgresConnectorConfig config = new PostgresConnectorConfig(defaultConfig().build());
        try (PostgresConnection connection = new PostgresConnection(config.getJdbcConfig(), getPostgresValueConverterBuilder(config), CONNECTION_TEST)) {
            return connection.getDatabaseCharset();
        }
    }

    public static PostgresSchema getSchema(PostgresConnectorConfig config) {
        return getSchema(config, TestHelper.getTypeRegistry());
    }

    public static PostgresSchema getSchema(PostgresConnectorConfig config, TypeRegistry typeRegistry) {
        return new PostgresSchema(
                config,
                TestHelper.getDefaultValueConverter(),
                (TopicNamingStrategy) SchemaTopicNamingStrategy.create(config),
                getPostgresValueConverter(typeRegistry, config), config.getServiceRegistry().tryGetService(CustomConverterRegistry.class));
    }

    protected static Set<String> schemaNames() throws SQLException {
        try (PostgresConnection connection = create()) {
            return connection.readAllSchemaNames(((Predicate<String>) Arrays.asList("pg_catalog", "information_schema")::contains).negate());
        }
    }

    public static JdbcConfiguration defaultJdbcConfig(String hostname, int port) {
        return defaultJdbcConfigBuilder(hostname, port)
                .build();
    }

    public static JdbcConfiguration.Builder defaultJdbcConfigBuilder(String hostname, int port) {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .with(CommonConnectorConfig.TOPIC_PREFIX, "dbserver1")
                .withDefault(JdbcConfiguration.DATABASE, "postgres")
                .withDefault(JdbcConfiguration.HOSTNAME, hostname)
                .withDefault(JdbcConfiguration.PORT, port)
                .withDefault(JdbcConfiguration.USER, "postgres")
                .withDefault(JdbcConfiguration.PASSWORD, "postgres");
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return defaultJdbcConfig("localhost", 5432);
    }

    public static JdbcConfiguration.Builder defaultJdbcConfigBuilder() {
        return defaultJdbcConfigBuilder("localhost", 5432);
    }

    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));
        builder.with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 100)
                .with(PostgresConnectorConfig.PLUGIN_NAME, decoderPlugin())
                .with(PostgresConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED)
                .with(PostgresConnectorConfig.MAX_RETRIES, 2)
                .with(PostgresConnectorConfig.RETRY_DELAY_MS, 2000);
        final String testNetworkTimeout = System.getProperty(TEST_PROPERTY_PREFIX + "network.timeout");
        if (testNetworkTimeout != null && testNetworkTimeout.length() != 0) {
            builder.with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, Integer.parseInt(testNetworkTimeout));
        }
        return builder;
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (PostgresConnection connection = create()) {
            connection.execute(statements);
        }
    }

    public static String topicName(String suffix) {
        return TestHelper.TEST_SERVER + "." + suffix;
    }

    protected static boolean shouldSSLConnectionFail() {
        return Boolean.parseBoolean(System.getProperty(TEST_PROPERTY_PREFIX + "ssl.failonconnect", "true"));
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    protected static SourceInfo sourceInfo() {
        return new SourceInfo(new PostgresConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                        .with(PostgresConnectorConfig.DATABASE_NAME, TEST_DATABASE)
                        .build()));
    }

    protected static void createDefaultReplicationSlot() {
        try {
            execute(String.format(
                    "SELECT * FROM pg_create_logical_replication_slot('%s', '%s')",
                    ReplicationConnection.Builder.DEFAULT_SLOT_NAME,
                    decoderPlugin().getPostgresPluginName()));
        }
        catch (Exception e) {
            LOGGER.debug("Error while creating default replication slot", e);
        }
    }

    protected static SlotState getDefaultReplicationSlot() {
        try (PostgresConnection connection = create()) {
            return connection.getReplicationSlotState(ReplicationConnection.Builder.DEFAULT_SLOT_NAME,
                    decoderPlugin().getPostgresPluginName());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropDefaultReplicationSlot() {
        try {
            execute("SELECT pg_drop_replication_slot('" + ReplicationConnection.Builder.DEFAULT_SLOT_NAME + "')");
        }
        catch (Exception e) {
            if (!Throwables.getRootCause(e).getMessage().startsWith("ERROR: replication slot \"debezium\" does not exist")) {
                throw e;
            }
        }
    }

    public static void dropPublication() {
        dropPublication(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME);
    }

    protected static void createPublicationForAllTables() {
        createPublicationForAllTables(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME);
    }

    protected static void dropPublication(String publicationName) {
        if (decoderPlugin().equals(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)) {
            try {
                execute("DROP PUBLICATION " + publicationName);
            }
            catch (Exception e) {
                LOGGER.debug("Error while dropping publication: '" + publicationName + "'", e);
            }
        }
    }

    protected static void createPublicationForAllTables(String publicationName) {
        if (decoderPlugin().equals(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)) {
            execute("CREATE PUBLICATION " + publicationName + " FOR ALL TABLES");
        }
    }

    protected static boolean publicationExists() {
        return publicationExists(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME);
    }

    protected static boolean publicationExists(String publicationName) {
        if (decoderPlugin().equals(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)) {
            try (PostgresConnection connection = create()) {
                String query = String.format("SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = '%s'", publicationName);
                try {
                    return connection.queryAndMap(query, ResultSet::next);
                }
                catch (SQLException e) {
                    // ignored
                }
            }
        }
        return false;
    }

    protected static void waitForDefaultReplicationSlotBeActive() {
        try (PostgresConnection connection = create()) {
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> connection.prepareQueryAndMap(
                    "select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ? and active = true", statement -> {
                        statement.setString(1, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
                        statement.setString(2, "postgres");
                        statement.setString(3, TestHelper.decoderPlugin().getPostgresPluginName());
                    },
                    rs -> rs.next()));
        }
    }

    protected static void setReplicaIdentityForTable(String table, String identity) {
        execute(String.format("ALTER TABLE %s REPLICA IDENTITY %s;", table, identity));
        try (PostgresConnection connection = create()) {
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> connection
                    .prepareQueryAndMap("SELECT relreplident FROM pg_class WHERE oid = ?::regclass;", statement -> {
                        statement.setString(1, table);
                    }, rs -> {
                        if (!rs.next()) {
                            return false;
                        }
                        return identity.toLowerCase().startsWith(rs.getString(1));
                    }));
        }
    }

    protected static void assertNoOpenTransactions() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            connection.setAutoCommit(true);

            try {
                Awaitility.await()
                        .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                        .until(() -> getOpenIdleTransactions(connection).size() == 0);
            }
            catch (ConditionTimeoutException e) {
                fail("Expected no open transactions but there was at least one.");
            }
        }
    }

    private static List<String> getOpenIdleTransactions(PostgresConnection connection) throws SQLException {
        int connectionPID = ((PgConnection) connection.connection()).getBackendPID();
        return connection.queryAndMap(
                "SELECT state FROM pg_stat_activity WHERE state like 'idle in transaction' AND pid <> " + connectionPID,
                rs -> {
                    final List<String> ret = new ArrayList<>();
                    while (rs.next()) {
                        ret.add(rs.getString(1));
                    }
                    return ret;
                });
    }

    private static PostgresValueConverter getPostgresValueConverter(TypeRegistry typeRegistry, PostgresConnectorConfig config) {
        return getPostgresValueConverterBuilder(config).build(typeRegistry);
    }

    private static PostgresValueConverterBuilder getPostgresValueConverterBuilder(PostgresConnectorConfig config) {
        return typeRegistry -> new PostgresValueConverter(
                Charset.forName("UTF-8"),
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                config.includeUnknownDatatypes(),
                typeRegistry,
                config.hStoreHandlingMode(),
                config.binaryHandlingMode(),
                config.intervalHandlingMode(),
                new UnchangedToastedPlaceholder(config),
                config.moneyFractionDigits());
    }
}

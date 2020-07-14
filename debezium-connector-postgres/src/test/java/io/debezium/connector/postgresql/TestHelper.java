/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SecureConnectionMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A utility for integration test cases to connect the PostgreSQL server running in the Docker container created by this module's
 * build.
 *
 * @author Horia Chiorean
 */
public final class TestHelper {

    protected static final String TEST_SERVER = "test_server";
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

    private TestHelper() {
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
        final PostgresConnectorConfig.LogicalDecoder plugin = decoderPlugin();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(defaultConfig().build());
        return ReplicationConnection.builder(defaultJdbcConfig())
                .withPlugin(plugin)
                .withSlot(slotName)
                .withTypeRegistry(getTypeRegistry())
                .dropSlotOnClose(dropOnClose)
                .statusUpdateInterval(Duration.ofSeconds(10))
                .withSchema(getSchema(config))
                .build();
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
        return new PostgresConnection(defaultJdbcConfig());
    }

    /**
     * Obtain a DB connection providing type registry.
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection createWithTypeRegistry() {
        return new PostgresConnection(defaultJdbcConfig(), true);
    }

    /**
     * Obtain a DB connection with a custom application name.
     *
     * @param appName the name of the application used for PostgreSQL diagnostics
     *
     * @return the PostgresConnection instance; never null
     */
    public static PostgresConnection create(String appName) {
        return new PostgresConnection(defaultJdbcConfig().edit().with("ApplicationName", appName).build());
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
        try (final PostgresConnection connection = new PostgresConnection(defaultJdbcConfig(), true)) {
            return connection.getTypeRegistry();
        }
    }

    public static PostgresSchema getSchema(PostgresConnectorConfig config) {
        return getSchema(config, TestHelper.getTypeRegistry());
    }

    public static PostgresSchema getSchema(PostgresConnectorConfig config, TypeRegistry typeRegistry) {
        return new PostgresSchema(
                config,
                typeRegistry,
                Charset.forName("UTF-8"),
                PostgresTopicSelector.create(config));
    }

    protected static Set<String> schemaNames() throws SQLException {
        try (PostgresConnection connection = create()) {
            return connection.readAllSchemaNames(Filters.IS_SYSTEM_SCHEMA.negate());
        }
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "postgres")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 5432)
                .withDefault(JdbcConfiguration.USER, "postgres")
                .withDefault(JdbcConfiguration.PASSWORD, "postgres")
                .build();
    }

    protected static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with(PostgresConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));
        builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 100)
                .with(PostgresConnectorConfig.PLUGIN_NAME, decoderPlugin())
                .with(PostgresConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED);
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
            connection.executeWithoutCommitting(statements);
        }
    }

    protected static String topicName(String suffix) {
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
                        .with(PostgresConnectorConfig.SERVER_NAME, TEST_SERVER)
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
            LOGGER.debug("Error while dropping default replication slot", e);
        }
    }

    protected static void dropDefaultReplicationSlot() {
        try {
            execute("SELECT pg_drop_replication_slot('" + ReplicationConnection.Builder.DEFAULT_SLOT_NAME + "')");
        }
        catch (Exception e) {
            LOGGER.debug("Error while dropping default replication slot", e);
        }
    }

    protected static void dropPublication() {
        dropPublication(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME);
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

    protected static void assertNoOpenTransactions() throws SQLException {
        try (PostgresConnection connection = TestHelper.create()) {
            connection.setAutoCommit(true);

            try {
                Awaitility.await()
                        .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                        .until(() -> getOpenIdleTransactions(connection).size() == 0);
            }
            catch (ConditionTimeoutException e) {
            }
            assertThat(getOpenIdleTransactions(connection)).hasSize(0);
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

}

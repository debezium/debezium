/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver.util;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceTimestampMode;
import io.debezium.connector.sqlserver.SqlServerChangeTable;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.IoUtil;
import io.debezium.util.Strings;
import io.debezium.util.Testing;

/**
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASE = "testDB";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final String ENABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=0)\n"
            + "EXEC sys.sp_cdc_enable_db";
    private static final String DISABLE_DB_CDC = "IF EXISTS(select 1 from sys.databases where name='#' AND is_cdc_enabled=1)\n"
            + "EXEC sys.sp_cdc_disable_db";
    private static final String ENABLE_TABLE_CDC = "IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=0)\n"
            + "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'#', @role_name = NULL, @supports_net_changes = 0";
    private static final String IS_CDC_ENABLED = "SELECT COUNT(1) FROM sys.databases WHERE name = '#' AND is_cdc_enabled=1";
    private static final String IS_CDC_TABLE_ENABLED = "SELECT COUNT(*) FROM sys.tables tb WHERE tb.is_tracked_by_cdc = 1 AND tb.name='#'";
    private static final String ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE = "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'%s', @capture_instance = N'%s', @role_name = NULL, @supports_net_changes = 0, @captured_column_list = %s";
    private static final String DISABLE_TABLE_CDC = "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'#', @capture_instance = 'all'";
    private static final String CDC_WRAPPERS_DML;

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

    static {
        try {
            ClassLoader classLoader = TestHelper.class.getClassLoader();
            CDC_WRAPPERS_DML = IoUtil.read(classLoader.getResourceAsStream("generate_cdc_wrappers.sql"));
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot load SQL Server statements", e);
        }
    }

    public static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                .build();
    }

    public static Configuration.Builder defaultConnectorConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "server1")
                .with(SqlServerConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    /**
     * Returns a default connector configuration suitable for most test cases. Can be amended/overridden
     * in individual tests as needed.
     */
    public static Configuration.Builder defaultConfig() {
        return defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAME.name(), TEST_DATABASE);
    }

    /**
     * Returns a default configuration for connectors in multi-partition mode.
     */
    public static Configuration.Builder defaultMultiPartitionConfig() {
        return defaultConnectorConfig()
                .with(SqlServerConnectorConfig.DATABASE_NAMES.name(), TEST_DATABASE);
    }

    public static void createTestDatabase() {
        createTestDatabase(TEST_DATABASE);
    }

    public static void createTestDatabase(String databaseName) {
        // NOTE: you cannot enable CDC for the "master" db (the default one) so
        // all tests must use a separate database...
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            dropTestDatabase(connection, databaseName);
            String sql = String.format("CREATE DATABASE [%s]\n", databaseName);
            connection.execute(sql);
            connection.execute(String.format("USE [%s]", databaseName));
            connection.execute(String.format("ALTER DATABASE [%s] SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName));
            // NOTE: you cannot enable CDC on master
            enableDbCdc(connection, databaseName);
        }
        catch (SQLException e) {
            LOGGER.error("Error while initiating test database", e);
            throw new IllegalStateException("Error while initiating test database", e);
        }
    }

    public static void dropTestDatabase() {
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            dropTestDatabase(connection, TEST_DATABASE);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Error while dropping test database", e);
        }
    }

    private static void dropTestDatabase(SqlServerConnection connection, String databaseName) throws SQLException {
        try {
            Awaitility.await("Disabling CDC").atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    connection.execute(String.format("USE [%s]", databaseName));
                }
                catch (SQLException e) {
                    // if the database doesn't yet exist, there is no need to disable CDC
                    return true;
                }
                try {
                    disableDbCdc(connection, databaseName);
                    return true;
                }
                catch (SQLException e) {
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException(String.format("Failed to disable CDC on %s", databaseName), e);
        }

        connection.execute("USE master");

        try {
            Awaitility.await(String.format("Dropping database %s", databaseName)).atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    String sql = String.format("IF EXISTS(select 1 from sys.databases where name = '%s') DROP DATABASE [%s]", databaseName, databaseName);
                    connection.execute(sql);
                    return true;
                }
                catch (SQLException e) {
                    LOGGER.warn(String.format("DROP DATABASE %s failed (will be retried): {}", databaseName), e.getMessage());
                    try {
                        connection.execute(String.format("ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", databaseName));
                    }
                    catch (SQLException e2) {
                        LOGGER.error("Failed to rollback immediately", e2);
                    }
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Failed to drop test database", e);
        }
    }

    public static SqlServerConnection adminConnection() {
        return new SqlServerConnection(TestHelper.adminJdbcConfig(), SourceTimestampMode.getDefaultMode(),
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null), () -> TestHelper.class.getClassLoader(),
                Collections.emptySet(), true);
    }

    public static SqlServerConnection testConnection() {
        return testConnection(TEST_DATABASE);
    }

    public static SqlServerConnection testConnection(String databaseName) {
        Configuration config = defaultJdbcConfig()
                .edit()
                .with(JdbcConfiguration.ON_CONNECT_STATEMENTS, "USE [" + databaseName + "]")
                .build();

        return new SqlServerConnection(config, SourceTimestampMode.getDefaultMode(),
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null), () -> TestHelper.class.getClassLoader(),
                Collections.emptySet(), true);
    }

    /**
     * Enables CDC for a given database, if not already enabled.
     *
     * @param name
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    public static void enableDbCdc(SqlServerConnection connection, String name) throws SQLException {
        try {
            Objects.requireNonNull(name);
            connection.execute(ENABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));

            // make sure testDB has cdc-enabled before proceeding; throwing exception if it fails
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
                final String sql = IS_CDC_ENABLED.replace(STATEMENTS_PLACEHOLDER, name);
                return connection.queryAndMap(sql, connection.singleResultMapper(rs -> rs.getLong(1), "")) == 1L;
            });
        }
        catch (SQLException e) {
            LOGGER.error("Failed to enable CDC on database " + name);
            throw e;
        }
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @param name
     *            the name of the DB, may not be {@code null}
     * @throws SQLException
     *             if anything unexpected fails
     */
    protected static void disableDbCdc(SqlServerConnection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        connection.execute(DISABLE_DB_CDC.replace(STATEMENTS_PLACEHOLDER, name));
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        String generateWrapperFunctionsStmts = CDC_WRAPPERS_DML.replaceAll(STATEMENTS_PLACEHOLDER, name.replaceAll("\\$", "\\\\\\$"));
        connection.execute(enableCdcForTableStmt, generateWrapperFunctionsStmts);
    }

    /**
     * @param name
     *            the name of the table, may not be {@code null}
     * @return true if CDC is enabled for the table
     * @throws SQLException if anything unexpected fails
     */
    public static boolean isCdcEnabled(SqlServerConnection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String tableEnabledStmt = IS_CDC_TABLE_ENABLED.replace(STATEMENTS_PLACEHOLDER, name);
        return connection.queryAndMap(
                tableEnabledStmt,
                connection.singleResultMapper(rs -> rs.getInt(1) > 0, "Cannot get CDC status of the table"));
    }

    /**
     * Enables CDC for a table with a custom capture name
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @param captureName
     *            the name of the capture instance, may not be {@code null}
     *
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String tableName, String captureName) throws SQLException {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        String enableCdcForTableStmt = String.format(ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE, tableName, captureName, "NULL");
        connection.execute(enableCdcForTableStmt);
    }

    /**
     * Enables CDC for a table with a custom capture name
     * functions for that table.
     *
     * @param connection
     *            sql connection
     * @param tableName
     *            the name of the table, may not be {@code null}
     * @param captureName
     *            the name of the capture instance, may not be {@code null}
     * @param captureColumnList
     *            the source table columns that are to be included in the change table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String tableName, String captureName, List<String> captureColumnList) throws SQLException {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        Objects.requireNonNull(captureColumnList);
        String captureColumnListParam = String.format("N'%s'", Strings.join(",", captureColumnList));
        String enableCdcForTableStmt = String.format(ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE, tableName, captureName, captureColumnListParam);
        connection.execute(enableCdcForTableStmt);
    }

    /**
     * Disables CDC for a table for which it was enabled before.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void disableTableCdc(SqlServerConnection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String disableCdcForTableStmt = DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        connection.execute(disableCdcForTableStmt);
    }

    public static void waitForSnapshotToBeCompleted() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            Awaitility.await("Snapshot not completed").atMost(Duration.ofSeconds(60)).until(() -> {
                try {
                    return (boolean) mbeanServer.getAttribute(getObjectName("snapshot", "server1"), "SnapshotCompleted");
                }
                catch (InstanceNotFoundException e) {
                    // Metrics has not started yet
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("Snapshot did not complete", e);
        }
    }

    public static void waitForStreamingStarted() {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            Awaitility.await("Streaming never started").atMost(Duration.ofSeconds(60)).until(() -> {
                try {
                    return (boolean) mbeanServer.getAttribute(getObjectName("streaming", "server1"), "Connected");
                }
                catch (InstanceNotFoundException e) {
                    // Metrics has not started yet
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("Streaming did not start", e);
        }
    }

    public static void waitForMaxLsnAvailable(SqlServerConnection connection) throws Exception {
        waitForMaxLsnAvailable(connection, TEST_DATABASE);
    }

    public static void waitForMaxLsnAvailable(SqlServerConnection connection, String databaseName) throws Exception {
        try {
            Awaitility.await("Max LSN not available")
                    .atMost(60, TimeUnit.SECONDS)
                    .pollDelay(Duration.ofSeconds(0))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> connection.getMaxLsn(databaseName).isAvailable());
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("A max LSN was not available", e);
        }
    }

    private static ObjectName getObjectName(String context, String serverName) throws MalformedObjectNameException {
        return new ObjectName("debezium.sql_server:type=connector-metrics,context=" + context + ",server=" + serverName);
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "5"));
    }

    /**
     * Utility method that will poll the CDC change tables and provide the record handler with the changes detected.
     * The record handler can then make a determination as to whether to return {@code true} if the expected outcome
     * exists or {@code false} to indicate it did not find what it expected.  This method will block until either
     * the handler returns {@code true} or if the polling fails to complete within the allocated poll window.
     *
     * @param connection the SQL Server connection to be used
     * @param tableName the main table name to be checked
     * @param handler the handler method to be called if changes are found in the capture table instance
     */
    public static void waitForCdcRecord(SqlServerConnection connection, String tableName, CdcRecordHandler handler) {
        try {
            Awaitility.await("Checking for expected record in CDC table for " + tableName)
                    .atMost(60, TimeUnit.SECONDS)
                    .pollDelay(Duration.ofSeconds(0))
                    .pollInterval(Duration.ofMillis(100)).until(() -> {
                        if (!connection.getMaxLsn(TEST_DATABASE).isAvailable()) {
                            return false;
                        }

                        for (SqlServerChangeTable ct : connection.listOfChangeTables(TEST_DATABASE)) {
                            final String ctTableName = ct.getChangeTableId().table();
                            if (ctTableName.endsWith("dbo_" + connection.getNameOfChangeTable(tableName))) {
                                try {
                                    final Lsn minLsn = connection.getMinLsn(TEST_DATABASE, ctTableName);
                                    final Lsn maxLsn = connection.getMaxLsn(TEST_DATABASE);
                                    final CdcRecordFoundBlockingMultiResultSetConsumer consumer = new CdcRecordFoundBlockingMultiResultSetConsumer(handler);
                                    SqlServerChangeTable[] tables = Collections.singletonList(ct).toArray(new SqlServerChangeTable[]{});
                                    connection.getChangesForTables(TEST_DATABASE, tables, minLsn, maxLsn, consumer);
                                    return consumer.isFound();
                                }
                                catch (Exception e) {
                                    if (e.getMessage().contains("An insufficient number of arguments were supplied")) {
                                        // This can happen if the request to get changes for tables happens too quickly.
                                        // In this case, we're going to ignore it.
                                        return false;
                                    }
                                    throw new AssertionError("Failed to fetch changes for " + tableName, e);
                                }
                            }
                        }
                        return false;
                    });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Expected record never appeared in the CDC table", e);
        }
    }

    public static void waitForEnabledCdc(SqlServerConnection connection, String table) throws SQLException, InterruptedException {
        Awaitility
                .await("CDC " + table)
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> TestHelper.isCdcEnabled(connection, table));
    }

    public static void waitForDisabledCdc(SqlServerConnection connection, String table) throws SQLException, InterruptedException {
        Awaitility
                .await("CDC " + table)
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> !TestHelper.isCdcEnabled(connection, table));
    }

    public static void waitForCdcRecord(SqlServerConnection connection, String tableName, String captureInstanceName, CdcRecordHandler handler) {
        try {
            Awaitility.await("Checking for expected record in CDC table for " + tableName)
                    .atMost(30, TimeUnit.SECONDS)
                    .pollDelay(Duration.ofSeconds(0))
                    .pollInterval(Duration.ofMillis(100)).until(() -> {
                        if (!connection.getMaxLsn(TEST_DATABASE).isAvailable()) {
                            return false;
                        }

                        for (SqlServerChangeTable ct : connection.listOfChangeTables(TEST_DATABASE)) {
                            final String ctTableName = ct.getChangeTableId().table();
                            if (ctTableName.endsWith(connection.getNameOfChangeTable(captureInstanceName))) {
                                try {
                                    final Lsn minLsn = connection.getMinLsn(TEST_DATABASE, ctTableName);
                                    final Lsn maxLsn = connection.getMaxLsn(TEST_DATABASE);
                                    final CdcRecordFoundBlockingMultiResultSetConsumer consumer = new CdcRecordFoundBlockingMultiResultSetConsumer(handler);
                                    SqlServerChangeTable[] tables = Collections.singletonList(ct).toArray(new SqlServerChangeTable[]{});
                                    connection.getChangesForTables(TEST_DATABASE, tables, minLsn, maxLsn, consumer);
                                    return consumer.isFound();
                                }
                                catch (Exception e) {
                                    if (e.getMessage().contains("An insufficient number of arguments were supplied")) {
                                        // This can happen if the request to get changes for tables happens too quickly.
                                        // In this case, we're going to ignore it.
                                        return false;
                                    }
                                    throw new AssertionError("Failed to fetch changes for " + tableName, e);
                                }
                            }
                        }
                        return false;
                    });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Expected record never appeared in the CDC table", e);
        }
    }

    @FunctionalInterface
    public interface CdcRecordHandler {
        boolean apply(ResultSet rs) throws SQLException;
    }

    /**
     * A multiple result-set consumer used internally by {@link #waitForCdcRecord(SqlServerConnection, String, CdcRecordHandler)}
     * that allows returning whether the provided {@link CdcRecordHandler} detected the expected condition or not.
     */
    static class CdcRecordFoundBlockingMultiResultSetConsumer implements JdbcConnection.BlockingMultiResultSetConsumer {
        private final CdcRecordHandler handler;
        private boolean found;

        public CdcRecordFoundBlockingMultiResultSetConsumer(CdcRecordHandler handler) {
            this.handler = handler;
        }

        @Override
        public void accept(ResultSet[] rs) throws SQLException, InterruptedException {
            if (rs.length == 1) {
                final ResultSet resultSet = rs[0];
                while (resultSet.next()) {
                    if (handler.apply(resultSet)) {
                        this.found = true;
                        break;
                    }
                }
            }
        }

        public boolean isFound() {
            return found;
        }
    }
}

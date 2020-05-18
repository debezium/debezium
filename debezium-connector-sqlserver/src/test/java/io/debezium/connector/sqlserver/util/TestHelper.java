/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver.util;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SourceTimestampMode;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Clock;
import io.debezium.util.IoUtil;
import io.debezium.util.Metronome;
import io.debezium.util.Testing;

/**
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASE = "testdb";
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
    private static final String ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE = "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'%s', @capture_instance = N'%s', @role_name = NULL, @supports_net_changes = 0";
    private static final String DISABLE_TABLE_CDC = "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'#', @capture_instance = 'all'";
    private static final String CDC_WRAPPERS_DML;

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
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "master")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 1433)
                .withDefault(JdbcConfiguration.USER, "sa")
                .withDefault(JdbcConfiguration.PASSWORD, "Password!")
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
                (field, value) -> builder.with(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "server1")
                .with(SqlServerConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    public static void createTestDatabase() {
        // NOTE: you cannot enable CDC for the "master" db (the default one) so
        // all tests must use a separate database...
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            dropTestDatabase(connection);
            String sql = "CREATE DATABASE testDB\n";
            connection.execute(sql);
            connection.execute("USE testDB");
            connection.execute("ALTER DATABASE testDB SET ALLOW_SNAPSHOT_ISOLATION ON");
            // NOTE: you cannot enable CDC on master
            enableDbCdc(connection, "testDB");
        }
        catch (SQLException e) {
            LOGGER.error("Error while initiating test database", e);
            throw new IllegalStateException("Error while initiating test database", e);
        }
    }

    public static void dropTestDatabase() {
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            dropTestDatabase(connection);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Error while dropping test database", e);
        }
    }

    private static void dropTestDatabase(SqlServerConnection connection) throws SQLException {
        try {
            Awaitility.await("Disabling CDC").atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    connection.execute("USE testDB");
                }
                catch (SQLException e) {
                    // if the database doesn't yet exist, there is no need to disable CDC
                    return true;
                }
                try {
                    disableDbCdc(connection, "testDB");
                    return true;
                }
                catch (SQLException e) {
                    return false;
                }
            });
        }
        catch (ConditionTimeoutException e) {
            throw new IllegalArgumentException("Failed to disable CDC on testDB", e);
        }

        connection.execute("USE master");

        try {
            Awaitility.await("Dropping database testDB").atMost(60, TimeUnit.SECONDS).until(() -> {
                try {
                    String sql = "IF EXISTS(select 1 from sys.databases where name = 'testDB') DROP DATABASE testDB";
                    connection.execute(sql);
                    return true;
                }
                catch (SQLException e) {
                    LOGGER.warn("DROP DATABASE testDB failed (will be retried): {}", e.getMessage());
                    try {
                        connection.execute("ALTER DATABASE testDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;");
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
        return new SqlServerConnection(TestHelper.adminJdbcConfig(), Clock.system(), SourceTimestampMode.getDefaultMode());
    }

    public static SqlServerConnection testConnection() {
        return new SqlServerConnection(TestHelper.defaultJdbcConfig(), Clock.system(), SourceTimestampMode.getDefaultMode());
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
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(SqlServerConnection connection, String tableName, String captureName) throws SQLException {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(captureName);
        String enableCdcForTableStmt = String.format(ENABLE_TABLE_CDC_WITH_CUSTOM_CAPTURE, tableName, captureName);
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

    public static void waitForSnapshotToBeCompleted() throws InterruptedException {
        int waitForSeconds = 60;
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.system());

        while (true) {
            if (waitForSeconds-- <= 0) {
                Assert.fail("Snapshot was not completed on time");
            }
            try {
                final boolean completed = (boolean) mbeanServer.getAttribute(new ObjectName("debezium.sql_server:type=connector-metrics,context=snapshot,server=server1"),
                        "SnapshotCompleted");
                if (completed) {
                    break;
                }
            }
            catch (InstanceNotFoundException e) {
                // Metrics has not started yet
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            metronome.pause();
        }
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "5"));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver.util;

import java.nio.file.Path;
import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectionFactory;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class TestHelper {
    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASE = "testDB";

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
                (field, value) -> builder.with(SqlServerConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder.with(SqlServerConnectorConfig.LOGICAL_NAME, "server1")
                .with(SqlServerConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    public static void createTestDatabase() {
        // NOTE: you cannot enable CDC for the "master" db (the default one) so
        // all tests must use a separate database...
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            try {
                connection.execute("USE testDB");
                connection.disableDbCdc("testDB");
            }
            catch (SQLException e) {
            }
            connection.execute("USE master");
            String sql = "IF EXISTS(select 1 from sys.databases where name='testDB') DROP DATABASE testDB\n"
                    + "CREATE DATABASE testDB\n";
            connection.execute(sql);
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            connection.enableDbCdc("testDB");
        }
        catch (SQLException e) {
            throw new IllegalStateException("Error while initating test database", e);
        }
    }

    public static void dropTestDatabase() {
        try (SqlServerConnection connection = adminConnection()) {
            connection.connect();
            try {
                connection.execute("USE testDB");
                connection.disableDbCdc("testDB");
            }
            catch (SQLException e) {
            }
            connection.execute("USE master");
            String sql = "IF EXISTS(select 1 from sys.databases where name='testDB') DROP DATABASE testDB";
            connection.execute(sql);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Error while dropping test database", e);
        }
    }

    public static SqlServerConnection adminConnection() {
        return new SqlServerConnection(TestHelper.adminJdbcConfig(), new SqlServerConnectionFactory());
    }

    public static SqlServerConnection testConnection() {
        return new SqlServerConnection(TestHelper.defaultJdbcConfig(), new SqlServerConnectionFactory());
    }

}

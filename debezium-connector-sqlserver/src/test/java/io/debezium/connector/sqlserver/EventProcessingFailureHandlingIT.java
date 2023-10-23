/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium SQL Server connector.
 * The test will execute steps to desynchronize in-memory schema representation
 * with actual database schema which will cause an error in processing of events.
 *
 * @author Jiri Pechanec
 */
public class EventProcessingFailureHandlingIT extends AbstractConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb BIGINT NOT NULL)",
                "CREATE TABLE tablec (id int primary key, colc varchar(30))");
        TestHelper.enableTableCdc(connection, "tablea");
        TestHelper.enableTableCdc(connection, "tableb");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void warn() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, EventProcessingFailureHandlingMode.WARN)
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(EventDispatcher.class);

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO tablea VALUES (1, 'seed')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNull();

        // Will allow insertion of strings into what was originally a BIGINT NOT NULL column
        // This will cause NumberFormatExceptions which return nulls and thus an error due to the column being NOT NULL
        connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb varchar(30)");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNull();

        Awaitility.await()
                .alias("Found warning message in logs")
                .atMost(TestHelper.waitTimeForLogEntries(), TimeUnit.SECONDS).until(() -> {
                    return logInterceptor.containsWarnMessage("Error while processing event at offset {");
                });
    }

    @Test
    public void ignore() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(SqlServerConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, EventProcessingFailureHandlingMode.SKIP)
                .build();

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO tablea VALUES (1, 'seed')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNull();

        // Will allow insertion of strings into what was originally a BIGINT NOT NULL column
        // This will cause NumberFormatExceptions which return nulls and thus an error due to the column being NOT NULL
        connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb varchar(30)");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNull();
    }

    @Test
    public void fail() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START_1 = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .build();
        final LogInterceptor logInterceptor = new LogInterceptor(ErrorHandler.class);

        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO tablea VALUES (1, 'seed')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tablea")).hasSize(1);
        assertThat(records.recordsForTopic("server1.testDB1.dbo.tableb")).isNull();

        // Will allow insertion of strings into what was originally a BIGINT NOT NULL column
        // This will cause NumberFormatExceptions which return nulls and thus an error due to the column being NOT NULL
        connection.execute("ALTER TABLE dbo.tableb ALTER COLUMN colb varchar(30)");

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        Awaitility.await()
                .alias("Found error message in logs")
                .atMost(TestHelper.waitTimeForLogEntries(), TimeUnit.SECONDS).until(() -> {
                    boolean foundErrorMessageInLogs = logInterceptor.containsStacktraceElement("Error while processing event at offset {");
                    return foundErrorMessageInLogs && !isRunning.get();
                });
    }
}

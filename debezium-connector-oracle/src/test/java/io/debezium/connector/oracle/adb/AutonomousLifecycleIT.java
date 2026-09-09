/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.adb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.OracleConnectorTask;
import io.debezium.connector.oracle.junit.SkipWhenNotAutonomous;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Integration tests for the connector start-up behavior against an Oracle Autonomous Database.
 *
 * @author Chris Cranford
 */
@SkipWhenNotAutonomous(reason = "Tests verify Autonomous Database specific start-up behavior")
public class AutonomousLifecycleIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropTable(connection, "dbz1106");
        connection.execute("CREATE TABLE dbz1106 (id numeric(9,0), data varchar2(50), primary key(id))");
        TestHelper.streamTable(connection, "dbz1106");
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz1106");
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#1106")
    public void shouldAutoEnableArchiveLogOnlyModeWhenNotConfigured() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(OracleConnectorTask.class);

        // Deliberately does not configure log.mining.archive.log.only.mode
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1106")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        assertThat(logInterceptor.containsMessage("Oracle Autonomous Database detected, enabling archive-log-only mode")).isTrue();

        TestHelper.forceStreamingVisibility();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Test
    @FixFor("debezium/dbz#1106")
    public void shouldFailToStartWhenArchiveLogOnlyModeExplicitlyDisabled() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1106")
                .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, false)
                .build();

        final AtomicReference<Throwable> exception = new AtomicReference<>();
        start(OracleConnector.class, config, (success, message, error) -> exception.set(error));

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> exception.get() != null);
        assertThat(exception.get()).hasStackTraceContaining("explicitly set to 'false'");
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("debezium/dbz#1106")
    public void shouldFailToStartWhenPluggableDatabaseConfigured() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1106")
                .with(OracleConnectorConfig.PDB_NAME, "ORCLPDB1")
                .build();

        final AtomicReference<Throwable> exception = new AtomicReference<>();
        start(OracleConnector.class, config, (success, message, error) -> exception.set(error));

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> exception.get() != null);
        assertThat(exception.get()).hasStackTraceContaining("does not support pluggable database");
        assertConnectorNotRunning();
    }
}
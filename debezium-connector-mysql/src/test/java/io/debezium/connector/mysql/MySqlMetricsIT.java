/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class MySqlMetricsIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-metrics.txt").toAbsolutePath();
    private static final String SERVER_NAME = "myserver";
    private final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "connector_metrics_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private static final String INSERT1 = "INSERT INTO simple (val) VALUES (25);";
    private static final String INSERT2 = "INSERT INTO simple (val) VALUES (50);";

    @Before
    public void before() throws Exception {
        Testing.Print.enable();
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        // start connector
        start(MySqlConnector.class,
                DATABASE.defaultConfig()
                        .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                        .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                        .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                        .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("simple"))
                        .with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE)
                        .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE)
                        .build());

        assertConnectorIsRunning();

        // These methods use the JMX metrics, this simply checks they're available as expected
        waitForSnapshotToBeCompleted();
        waitForStreamingToStart();

        // Stop the connector
        stopConnector();

        // Verify snapshot metrics no longer exist
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getMBeanInfo(getSnapshotMetricsObjectName());
            Assert.fail("Expected Snapshot Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }

        // Verify streaming metrics no longer exist
        try {
            mBeanServer.getMBeanInfo(getStreamingMetricsObjectName());
            Assert.fail("Expected Streaming Metrics no longer to exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testSnapshotOnlyMetrics() throws Exception {
        // Setup
        try (Connection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            connection.createStatement().execute(INSERT1);
            connection.createStatement().execute(INSERT2);
        }

        // start connector
        start(MySqlConnector.class,
                DATABASE.defaultConfig()
                        .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_ONLY)
                        .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                        .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                        .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("simple"))
                        .with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE)
                        .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE)
                        .build());

        assertSnapshotMetrics();
        assertStreamingMetricsExist();
    }

    @Test
    public void testPauseResumeSnapshotMetrics() throws Exception {
        final int NUM_RECORDS = 1_000;
        final String TABLE_NAME = DATABASE.qualifiedTableName("simple");
        final String SIGNAL_TABLE_NAME = DATABASE.qualifiedTableName("debezium_signal");

        try (Connection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            for (int i = 1; i < NUM_RECORDS; i++) {
                connection.createStatement().execute(String.format("INSERT INTO %s (val) VALUES (%d);", TABLE_NAME, i));
            }
        }

        // Start connector.
        start(MySqlConnector.class,
                DATABASE.defaultConfig()
                        .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                        .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                        .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                        .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, String.format("%s", TABLE_NAME))
                        .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE)
                        .with(CommonConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 1)
                        .with(MySqlConnectorConfig.SIGNAL_DATA_COLLECTION, SIGNAL_TABLE_NAME)
                        .build());

        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();
        waitForStreamingToStart();

        // Consume initial snapshot records.
        List<SourceRecord> records = new ArrayList<>();
        consumeRecords(NUM_RECORDS, record -> {
            records.add(record);
        });

        try (Connection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            // Start incremental snapshot.
            connection.createStatement().execute(String.format(
                    "INSERT INTO debezium_signal VALUES('ad-hoc', 'execute-snapshot', '{\"data-collections\": [\"%s\"]}')", TABLE_NAME));
            // Pause incremental snapshot.
            connection.createStatement().execute(String.format("INSERT INTO %s VALUES('test-pause', 'pause-snapshot', '')", SIGNAL_TABLE_NAME));
            // Sleep more than 1 second, we get the pause in seconds.
            Thread.sleep(1500);
            // Resume incremental snapshot.
            connection.createStatement().execute(String.format("INSERT INTO debezium_signal VALUES('test-resume', 'resume-snapshot', '')", SIGNAL_TABLE_NAME));
        }

        // Consume incremental snapshot records.
        consumeRecords(NUM_RECORDS, record -> {
            records.add(record);
        });

        Assert.assertTrue(records.size() >= 2 * NUM_RECORDS);
        assertSnapshotPauseNotZero();
    }

    @Test
    public void testSnapshotAndStreamingMetrics() throws Exception {
        // Setup
        try (Connection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            connection.createStatement().execute(INSERT1);
            connection.createStatement().execute(INSERT2);
        }

        // start connector
        start(MySqlConnector.class,
                DATABASE.defaultConfig()
                        .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                        .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                        .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                        .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("simple"))
                        .with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE)
                        .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE)
                        .build());

        assertSnapshotMetrics();
        assertStreamingMetrics(0);
    }

    @Test
    public void testStreamingOnlyMetrics() throws Exception {
        // start connector
        start(MySqlConnector.class,
                DATABASE.defaultConfig()
                        .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                        .with(MySqlConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                        .with(FileSchemaHistory.FILE_PATH, SCHEMA_HISTORY_PATH)
                        .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("simple"))
                        .with(MySqlConnectorConfig.TABLES_IGNORE_BUILTIN, Boolean.TRUE)
                        .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, Boolean.TRUE)
                        .build());

        // CREATE DATABASE, CREATE TABLE, and 2 INSERT
        assertStreamingMetrics(4);
        assertSnapshotMetricsExist();
    }

    private void assertNoSnapshotMetricsExist() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted");
            Assert.fail("Expected Snapshot Metrics to not exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }
    }

    private void assertNoStreamingMetricsExist() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getAttribute(getStreamingMetricsObjectName(), "TotalNumberOfEventsSeen");
            Assert.fail("Expected Streaming Metrics to not exist");
        }
        catch (InstanceNotFoundException e) {
            // expected
        }
    }

    private void assertStreamingMetricsExist() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getAttribute(getStreamingMetricsObjectName(), "TotalNumberOfEventsSeen");
        }
        catch (InstanceNotFoundException e) {
            Assert.fail("Streaming Metrics should exist");
        }
    }

    private void assertSnapshotMetricsExist() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted");
        }
        catch (InstanceNotFoundException e) {
            Assert.fail("Snapshot Metrics should exist");
        }
    }

    private void assertSnapshotPauseNotZero() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            final long snapshotPauseDuration = (Long) mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPausedDurationInSeconds");
            Assert.assertTrue(snapshotPauseDuration > 0);
        }
        catch (InstanceNotFoundException e) {
            Assert.fail("Snapshot Metrics should exist");
        }
    }

    private void assertSnapshotMetrics() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the snapshot to complete to verify metrics
        waitForSnapshotToBeCompleted();

        // 4 meta, 1 USE, 1 CREATE, 2 INSERT
        consumeRecords(8);

        // Check snapshot metrics
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalTableCount")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "CapturedTables"))
                .isEqualTo(new String[]{ DATABASE.qualifiedTableName("simple") });
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "TotalNumberOfEventsSeen")).isEqualTo(2L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "NumberOfEventsFiltered")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "NumberOfErroneousEvents")).isEqualTo(0L);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "RemainingTableCount")).isEqualTo(0);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotRunning")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotAborted")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotCompleted")).isEqualTo(true);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPaused")).isEqualTo(false);
        assertThat(mBeanServer.getAttribute(getSnapshotMetricsObjectName(), "SnapshotPausedDurationInSeconds")).isEqualTo(0L);
    }

    private void assertStreamingMetrics(long events) throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Wait for the streaming to begin
        waitForStreamingToStart();

        // Insert new records and wait for them to become available
        try (Connection connection = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            connection.createStatement().execute(INSERT1);
            connection.createStatement().execute(INSERT2);
        }

        waitForAvailableRecords(30, TimeUnit.SECONDS);

        Testing.Print.enable();
        int size = consumeAvailableRecords(VerifyRecord::print);

        // Check streaming metrics
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "Connected")).isEqualTo(true);
        // note: other connectors would report the physical number of operations, e.g. inserts/updates.
        // the MySQL BinaryLogClientStatistics bean which this value is based upon tracks number of events
        // read from the log, which may be more than the insert/update/delete operations.
        assertThat((Long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "TotalNumberOfEventsSeen"))
                .isGreaterThanOrEqualTo(events);

        Awaitility.await().atMost(Duration.ofMinutes(1)).until(() -> ((String[]) mBeanServer
                .getAttribute(getStreamingMetricsObjectName(), "CapturedTables")).length > 0);
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CapturedTables"))
                .isEqualTo(new String[]{ DATABASE.qualifiedTableName("simple") });
    }

    private ObjectName getSnapshotMetricsObjectName() throws MalformedObjectNameException {
        return getSnapshotMetricsObjectName("mysql", SERVER_NAME);
    }

    private ObjectName getStreamingMetricsObjectName() throws MalformedObjectNameException {
        return getStreamingMetricsObjectName("mysql", SERVER_NAME, getStreamingNamespace());
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("mysql", SERVER_NAME);
    }

    private void waitForStreamingToStart() throws InterruptedException {
        waitForStreamingRunning("mysql", SERVER_NAME, getStreamingNamespace());
    }
}

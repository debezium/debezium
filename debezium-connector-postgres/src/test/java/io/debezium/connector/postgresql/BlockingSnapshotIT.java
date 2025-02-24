/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.SnapshotType.BLOCKING;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.pipeline.signal.channels.FileSignalChannel;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest<PostgresConnector> {

    private static final String TOPIC_NAME = "test_server.s1.a";

    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1;" +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.b (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.debezium_signal (id varchar(64), type varchar(32), data varchar(2048))";

    protected final Path signalsFile = Paths.get("src", "test", "resources")
            .resolve("debezium_signaling_blocking_file.txt");

    @Before
    public void before() throws SQLException {

        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        initializeConnectorTestFramework();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.createPublicationForAllTables();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5);
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {

        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_TABLES, "s1.a")
                .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, true);
    }

    @Override
    protected Class<PostgresConnector> connectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected String topicName() {
        return TOPIC_NAME;
    }

    @Override
    public List<String> topicNames() {
        return List.of(TOPIC_NAME, "test_server.s1.b");
    }

    @Override
    protected String tableName() {
        return "s1.a";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("s1.a", "s1.b");
    }

    @Override
    protected String signalTableName() {
        return "s1.debezium_signal";
    }

    @Override
    protected String escapedTableDataCollectionId() {
        return "\\\"s1\\\".\\\"a\\\"";
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

    @FixFor("DBZ-7311")
    @Test
    public void executeBlockingSnapshotWhenSnapshotModeIsNever() throws Exception {
        // Testing.Print.enable();

        // Avoid to start the streaming from data inserted before the connector start
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        populateTable();

        startConnector();

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        int signalingRecords = 1;

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT + signalingRecords, 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }

    @FixFor("DBZ-8680")
    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "idle_session_timeout not supported for PG version < 14")
    public void probeConnectionDuringBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        // This test ensures that the streaming connection is periodically probed while a blocking snapshot is taken. This helps reduce the risk
        // of an idle disconnect during the snapshot due to inactivity on the streaming connection. If such a disconnect were to occur, then it
        // would mean that the connector would fail to resume streaming after the snapshot is finished. What's worse: the snapshot completion
        // may not be committed, leading to an infinite snapshot loop with the connector failing after every snapshot attempt.

        // Avoid to start the streaming from data inserted before the connector start
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        populateTable();

        final long snapshotDelayMs = 10000;
        startConnector(x -> config()
                // Wait several seconds before starting the blocking snapshot. During this time, streaming has been paused, and could be at risk of
                // an idle disconnection if the connector does not keep the connection active.
                .with(CommonConnectorConfig.SNAPSHOT_DELAY_MS, snapshotDelayMs)
                // When streaming is paused, the streaming connection is idle. Disconnect it if it's idle for too long.
                // "options" is a PostgreSQL JDBC driver connection parameter that lets us pass through PG client configuration parameters.
                // While this is a simple mechanism for the test, in complex cloud environments there could always be other factors that lead to
                // idle TCP connections getting disconnected.
                //
                // For this test, the values must be lower than the snapshot delay, configured above.
                .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "options", "-c idle_session_timeout=5000 -c idle_in_transaction_session_timeout=5000")
                // The status update interval is also used as the frequency at which to probe the streaming connection. For this test, it must be
                // less than the idle timeouts configured on the JDBC driver, configured above.
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 2000)
                // Disable retries: if the test is going to fail, this makes it fail cleaner:
                .with(CommonConnectorConfig.ERRORS_MAX_RETRIES, 0)

        );

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, tableDataCollectionId());

        // Make sure the log message emitted by SNAPSHOT_DELAY_MS does get logged, thus ensuring that the snapshot took at least the configured
        // snapshot delay, so that we know idle timeouts could be reached in a failing test scenario.
        waitForLogMessage("The connector will wait for", AbstractSnapshotChangeEventSource.class);

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        int signalingRecords = 1;

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT + signalingRecords, 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }

    @FixFor("DBZ-7312")
    @Test
    public void executeBlockingSnapshotWhenASnapshotAlreadyExecuted() throws Exception {
        // Testing.Print.enable();

        // Avoid to start the streaming from data inserted before the connector start
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(true, true)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "not exist")
                .with(PostgresConnectorConfig.SLOT_NAME, "snapshot_mode_initial_crash4")

        );

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey("", "", BLOCKING, tableDataCollectionId());

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        int signalingRecords = 1;

        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT + signalingRecords, 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }

    @Test
    public void executeBlockingSnapshotJustAfterInitialSnapshotAndNoEventStreamedYet() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false)
                .with(FileSignalChannel.SIGNAL_FILE, signalsFile.toString())
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "file"));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        SourceRecords consumedRecordsByTopic = consumeRecordsByTopic(ROW_COUNT, 10);
        assertRecordsFromSnapshotAndStreamingArePresent(ROW_COUNT, consumedRecordsByTopic);

        sendExecuteSnapshotFileSignal(tableDataCollectionId(), BLOCKING.name(), signalsFile);

        waitForLogMessage("Snapshot completed", AbstractSnapshotChangeEventSource.class);

        assertRecordsFromSnapshotAndStreamingArePresent((ROW_COUNT), consumeRecordsByTopic((ROW_COUNT), 10));

        insertRecords(ROW_COUNT, ROW_COUNT * 2);

        assertStreamingRecordsArePresent(ROW_COUNT, consumeRecordsByTopic(ROW_COUNT, 10));

    }
}

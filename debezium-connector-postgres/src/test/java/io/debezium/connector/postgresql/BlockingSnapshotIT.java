/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.SnapshotType.BLOCKING;

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
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private static final String TOPIC_NAME = "test_server.s1.a";

    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1;" +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.b (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.debezium_signal (id varchar(64), type varchar(32), data varchar(2048))";

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
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
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
}

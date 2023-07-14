/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.SnapshotType.INITIAL_BLOCKING;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class BlockingSnapshotIT extends AbstractIncrementalSnapshotTest<PostgresConnector> {

    private static final String TOPIC_NAME = "test_server.s1.a";

    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1;CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.debezium_signal (id varchar(64), type varchar(32), data varchar(2048))";

    @Before
    public void before() throws SQLException {

        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        initializeConnectorTestFramework();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.execute("CREATE PUBLICATION dbz_publication FOR ALL TABLES;");
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

    }

    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4")
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a")
                // DBZ-4272 required to allow dropping columns just before an incremental snapshot
                .with("database.autosave", "conservative");
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
    protected void waitForConnectorToStart() {
        super.waitForConnectorToStart();
        TestHelper.waitForDefaultReplicationSlotBeActive();
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

    @Test
    public void executeBlockingSnapshot() throws Exception {
        // Testing.Print.enable();

        populateTable();

        startConnectorWithSnapshot(x -> mutableConfig(false, false));

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + ROW_COUNT + 1,
                        i + ROW_COUNT));
            }
            connection.commit();
        }

        SourceRecords snapshotAndStreamingRecords = consumeRecordsByTopic(ROW_COUNT * 2);
        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo(ROW_COUNT * 2);
        List<Integer> actual = snapshotAndStreamingRecords.recordsForTopic(topicName()).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32("aa"))
                .collect(Collectors.toList());
        assertThat(actual).containsAll(IntStream.range(0, 1999).boxed().collect(Collectors.toList()));

        sendAdHocSnapshotSignalWithAdditionalConditionWithSurrogateKey(Optional.empty(), Optional.empty(), INITIAL_BLOCKING, tableDataCollectionId());

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        snapshotAndStreamingRecords = consumeRecordsByTopic((ROW_COUNT * 2) + 1);
        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo((ROW_COUNT * 2) + 1);
        actual = snapshotAndStreamingRecords.recordsForTopic(topicName()).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32("aa"))
                .collect(Collectors.toList());
        assertThat(actual).containsAll(IntStream.range(0, 1999).boxed().collect(Collectors.toList()));

        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(false);
            for (int i = 0; i < ROW_COUNT; i++) {
                connection.executeWithoutCommitting(String.format("INSERT INTO %s (%s, aa) VALUES (%s, %s)",
                        tableName(),
                        connection.quotedColumnIdString(pkFieldName()),
                        i + (ROW_COUNT * 2) + 1,
                        i + (ROW_COUNT * 2)));
            }
            connection.commit();
        }

        snapshotAndStreamingRecords = consumeRecordsByTopic(ROW_COUNT + 1);
        assertThat(snapshotAndStreamingRecords.allRecordsInOrder().size()).isEqualTo(ROW_COUNT + 1);
        actual = snapshotAndStreamingRecords.recordsForTopic(topicName()).stream()
                .map(s -> ((Struct) s.value()).getStruct("after").getInt32("aa"))
                .collect(Collectors.toList());
        assertThat(actual).containsAll(IntStream.range(2000, 2999).boxed().collect(Collectors.toList()));
    }
}

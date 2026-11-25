/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Integration tests for the snapshot retry feature (DDD-59 / DBZ-2297).
 * <p>
 * Each test inserts rows into a table, starts a connector with {@code snapshot.retry.max > 0},
 * and terminates the snapshot backend mid-read via {@code pg_terminate_backend}. The connector
 * must recover and complete the snapshot with every row captured exactly once and the correct
 * FIRST / LAST snapshot markers.
 */
public class SnapshotRetryIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotRetryIT.class);

    private static final String TABLE_NAME = "snapshot_retry_test";
    private static final String QUALIFIED_TABLE_NAME = "public." + TABLE_NAME;
    private static final int ROW_COUNT = 2_000;

    private PostgresConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.createPublicationForAllTables();
        initializeConnectorTestFramework();

        connection = TestHelper.create();
        createAndPopulateTable();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("dbz#2297")
    public void shouldRetryAndCompleteSnapshotSingleThreaded() throws Exception {
        runRetryTest(1);
    }

    @Test
    @FixFor("dbz#2297")
    public void shouldRetryAndCompleteSnapshotMultiThreaded() throws Exception {
        runRetryTest(2);
    }

    private void runRetryTest(int snapshotMaxThreads) throws Exception {
        final Configuration config = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, QUALIFIED_TABLE_NAME)
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_RETRY_MAX, 3)
                .with(RelationalDatabaseConnectorConfig.SNAPSHOT_RETRY_DELAY_MS, 500)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, snapshotMaxThreads)
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT + 100)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 200)
                .build();

        // A latch that the killer thread uses to signal that it terminated a backend.
        final CountDownLatch killed = new CountDownLatch(1);
        final AtomicBoolean stopKiller = new AtomicBoolean(false);

        // Start a background thread that watches pg_stat_activity for the snapshot SELECT
        // and terminates the backend once found.
        final Thread killer = new Thread(() -> {
            try (PostgresConnection monitor = TestHelper.create()) {
                while (!stopKiller.get()) {
                    try {
                        int terminated = terminateSnapshotBackend(monitor);
                        if (terminated > 0) {
                            LOGGER.info("Terminated {} snapshot backend(s)", terminated);
                            killed.countDown();
                            return;
                        }
                    }
                    catch (SQLException e) {
                        LOGGER.debug("Monitor query failed, retrying", e);
                    }
                    Thread.sleep(50);
                }
            }
            catch (Exception e) {
                LOGGER.warn("Killer thread failed", e);
            }
        }, "snapshot-killer");
        killer.setDaemon(true);
        killer.start();

        try {
            start(PostgresConnector.class, config);
            assertConnectorIsRunning();

            // Wait for the snapshot to complete — the retry should handle the terminated backend.
            waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);

            final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT);
            final List<SourceRecord> tableRecords = allRecords.recordsForTopic(
                    TestHelper.topicName(QUALIFIED_TABLE_NAME));

            assertThat(tableRecords).as("all rows captured").hasSize(ROW_COUNT);

            // No duplicate keys
            final Set<Integer> keys = new HashSet<>();
            for (SourceRecord record : tableRecords) {
                Struct key = (Struct) record.key();
                int id = key.getInt32("id");
                assertThat(keys.add(id))
                        .as("duplicate key id=%d", id)
                        .isTrue();
            }

            // FIRST and LAST snapshot markers
            assertFirstAndLastMarkers(tableRecords);

            // The backend should have been terminated at least once.
            assertThat(killed.await(0, TimeUnit.SECONDS))
                    .as("backend was terminated during snapshot")
                    .isTrue();
        }
        finally {
            stopKiller.set(true);
            killer.interrupt();
            killer.join(5_000);
        }
    }

    private void createAndPopulateTable() throws SQLException {
        connection.execute("DROP TABLE IF EXISTS " + QUALIFIED_TABLE_NAME);
        connection.execute("CREATE TABLE " + QUALIFIED_TABLE_NAME
                + " (id integer PRIMARY KEY, data text)");
        try (PreparedStatement ps = connection.connection()
                .prepareStatement("INSERT INTO " + QUALIFIED_TABLE_NAME + " VALUES (?, ?)")) {
            for (int i = 1; i <= ROW_COUNT; i++) {
                ps.setInt(1, i);
                ps.setString(2, "row-" + i);
                ps.addBatch();
                if (i % 500 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        connection.commit();
    }

    /**
     * Finds snapshot backends reading our test table and terminates one of them.
     * Returns the number of backends terminated.
     */
    private int terminateSnapshotBackend(PostgresConnection monitor) throws SQLException {
        // Look for backends that are executing a SELECT on our test table and are NOT us.
        String sql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                + "WHERE pid <> pg_backend_pid() "
                + "AND state = 'active' "
                + "AND query LIKE '%" + TABLE_NAME + "%' "
                + "AND query NOT LIKE '%pg_stat_activity%' "
                + "LIMIT 1";
        try (java.sql.Statement stmt = monitor.connection().createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                if (rs.getBoolean(1)) {
                    count++;
                }
            }
            return count;
        }
    }

    private void assertFirstAndLastMarkers(List<SourceRecord> records) {
        assertThat(records).isNotEmpty();

        // The first record should carry a FIRST or FIRST_IN_DATA_COLLECTION marker.
        SourceRecord first = records.get(0);
        Struct firstSource = ((Struct) first.value()).getStruct(Envelope.FieldName.SOURCE);
        String firstSnapshot = firstSource.getString("snapshot");
        assertThat(firstSnapshot)
                .as("first record snapshot marker")
                .isIn("first", "first_in_data_collection");

        // The last record should carry a LAST or LAST_IN_DATA_COLLECTION marker.
        SourceRecord last = records.get(records.size() - 1);
        Struct lastSource = ((Struct) last.value()).getStruct(Envelope.FieldName.SOURCE);
        String lastSnapshot = lastSource.getString("snapshot");
        assertThat(lastSnapshot)
                .as("last record snapshot marker")
                .isIn("last", "last_in_data_collection");
    }
}

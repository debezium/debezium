/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenJavaVersion;
import io.debezium.pipeline.AbstractMetricsTest;

/**
 * @author Chris Cranford
 * @author Mario Fiore Vitale
 */
public class PostgresMetricsIT extends AbstractMetricsTest<PostgresConnector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresMetricsIT.class);

    private static final String INIT_STATEMENTS = "CREATE TABLE simple (pk SERIAL NOT NULL, val INT NOT NULL, PRIMARY KEY(pk)); "
            + "ALTER TABLE simple REPLICA IDENTITY FULL;";
    private static final String INSERT_STATEMENTS = "INSERT INTO simple (val) VALUES (25); "
            + "INSERT INTO simple (val) VALUES (50);";

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return "test_server";
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.ALWAYS)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE);
    }

    protected Configuration.Builder noSnapshot(Configuration.Builder config) {
        return config.with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected void executeInsertStatements() {
        TestHelper.execute(INSERT_STATEMENTS);
    }

    @Override
    protected String tableName() {
        return "public.simple";
    }

    @Override
    protected long expectedEvents() {
        return 2L;
    }

    @Override
    protected boolean snapshotCompleted() {
        return false;
    }

    @BeforeEach
    void before() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropAllSchemas();

        TestHelper.execute(INIT_STATEMENTS);
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
    }

    @Test
    @SkipWhenJavaVersion(check = EqualityCheck.GREATER_THAN_OR_EQUAL, value = 16, description = "Deep reflection not allowed by default on this Java version")
    public void oneRecordInQueue() throws Exception {
        // Testing.Print.enable();
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        executeInsertStatements();
        final CountDownLatch step1 = new CountDownLatch(1);
        final CountDownLatch step2 = new CountDownLatch(1);

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE, 10)
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .with(PostgresConnectorConfig.POLL_INTERVAL_MS, 100L)
                .with(PostgresConnectorConfig.MAX_QUEUE_SIZE_IN_BYTES, 10000L);
        start(PostgresConnector.class, configBuilder.build(), loggingCompletion(), null, x -> {
            LOGGER.info("Record '{}' arrived", x);
            step1.countDown();
            try {
                step2.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Record processing completed");
        }, true);

        waitForStreamingRunning(connector(), server());
        executeInsertStatements();
        LOGGER.info("Waiting for the first record to arrive");
        step1.await(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS);
        LOGGER.info("First record arrived");

        // One record is read from queue and reading is blocked
        // Second record should stay in queue
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value > 0;
                });
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isNotEqualTo(0L);

        LOGGER.info("Wait for the queue to contain second record");
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    int value = (int) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity");
                    return value == 9;
                });
        LOGGER.info("Wait for second record to be in queue");
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "QueueRemainingCapacity")).isEqualTo(9);

        LOGGER.info("Empty queue");
        step2.countDown();

        LOGGER.info("Wait for queue to be empty");
        Awaitility.await()
                .alias("MBean attribute was not an expected value")
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                .ignoreException(InstanceNotFoundException.class)
                .until(() -> {
                    long value = (long) mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes");
                    return value == 0;
                });
        assertThat(mBeanServer.getAttribute(getStreamingMetricsObjectName(), "CurrentQueueSizeInBytes")).isEqualTo(0L);
        stopConnector();
    }

    @Test
    public void testSourceEventPositionStalenessDuringLongRunningTransactionSkip() throws Exception {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // STEP 1: Start connector
        // Note: We need to keep the replication slot between stops for the restart to work
        Configuration.Builder configBuilder = noSnapshot(config())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning(connector(), server());

        LOGGER.info("=== First record consumed to have a baseline LSN ===");
        // STEP 0: First record consumed to have a baseline LSN
        TestHelper.execute("INSERT INTO simple (val) VALUES (99999);");

        waitForAvailableRecords(1, TimeUnit.SECONDS);
        consumeRecords(1);
        TabularData sourceEventPositionBeginning = (TabularData) mBeanServer.getAttribute(
                getStreamingMetricsObjectName(),
                "SourceEventPosition");
        String lsnBeginning = getLsnFromTabularData(sourceEventPositionBeginning);

        LOGGER.info("Beginning SourceEventPosition LSN: {}", formatLsn(lsnBeginning));

        LOGGER.info("=== Setting up long-running transaction scenario ===");

        // Open a separate connection for the long-running transaction
        try (PostgresConnection longTxConnection = TestHelper.create()) {
            // Start transaction A (this will hold a position in the replication slot)
            longTxConnection.setAutoCommit(false);
            longTxConnection.executeWithoutCommitting("INSERT INTO simple (val) VALUES (1000)");

            LOGGER.info("Transaction A started (long-running)");

            // Now execute and commit many other transactions while A is open
            LOGGER.info("Executing 100 transactions (B through E+98) while A is open...");
            for (int i = 0; i < 100; i++) {
                // Each of these is a separate transaction that commits immediately
                TestHelper.execute("INSERT INTO simple (val) VALUES (" + (2000 + i) + ");");
            }

            // Wait for Debezium to process these 100 transactions
            LOGGER.info("Waiting for 100 transactions to be processed...");
            waitForAvailableRecords(100, TimeUnit.SECONDS);
            consumeRecords(100);

            // Capture the LSN position after processing all these transactions
            TabularData afterShortTxsData = (TabularData) mBeanServer.getAttribute(
                    getStreamingMetricsObjectName(),
                    "SourceEventPosition");
            String lsnAfterShortTxs = getLsnFromTabularData(afterShortTxsData);
            LOGGER.info("After processing 100 short transactions, SourceEventPosition LSN: {}", formatLsn(lsnAfterShortTxs));

            // Add more data to transaction A (make it substantial)
            for (int i = 0; i < 50; i++) {
                longTxConnection.executeWithoutCommitting(
                        "INSERT INTO simple (val) VALUES (" + (10000 + i) + ")");
            }

            // Now COMMIT transaction A
            LOGGER.info("Committing transaction A...");
            longTxConnection.commit();

            // Wait for transaction A to be processed
            waitForAvailableRecords(50, TimeUnit.SECONDS); // 51 inserts from tx A
            consumeRecords(50);

            TabularData afterTxAData = (TabularData) mBeanServer.getAttribute(
                    getStreamingMetricsObjectName(),
                    "SourceEventPosition");
            String lsnAfterTxA = getLsnFromTabularData(afterTxAData);

            LOGGER.info("After processing transaction A, SourceEventPosition LSN: {}", formatLsn(lsnAfterTxA));

            // STEP 3: Stop connector (this triggers the problem scenario on restart)
            LOGGER.info("=== Stopping connector ===");
            stopConnector();

            // Query PostgreSQL to see the replication slot state
            try (PostgresConnection adminConn = TestHelper.create()) {
                adminConn.query(
                        "SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots " +
                                "WHERE slot_name = 'debezium'",
                        rs -> {
                            while (rs.next()) {
                                LOGGER.info("Replication slot state:");
                                LOGGER.info("  restart_lsn: {}", rs.getString("restart_lsn"));
                                LOGGER.info("  confirmed_flush_lsn: {}", rs.getString("confirmed_flush_lsn"));
                            }
                        });
            }

            // STEP 4: Restart connector - this triggers replay from transaction A's BEGIN
            LOGGER.info("=== Restarting connector - catchup/skip phase will begin ===");
            start(PostgresConnector.class, configBuilder.build());
            assertConnectorIsRunning();
            waitForStreamingRunning(connector(), server());

            // STEP 5: Monitor metrics DURING the skip phase
            // The connector is now:
            // 1. Reading from transaction A's BEGIN (very old LSN)
            // 2. Searching for lastCommitStoredLsn (recent LSN)
            // 3. Skipping all the already-processed transactions B-E+98
            // 4. SourceEventPosition should be STALE during this time

            LOGGER.info("=== Monitoring metrics during skip phase ===");

            // Give connector time to start processing/skipping
            Thread.sleep(1000);

            // Poll metrics multiple times during skip phase
            List<String> duringSkipLsns = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                try {
                    TabularData currentPosData = (TabularData) mBeanServer.getAttribute(
                            getStreamingMetricsObjectName(),
                            "SourceEventPosition");

                    String currentLsn = getLsnFromTabularData(currentPosData);
                    duringSkipLsns.add(currentLsn);

                    LOGGER.info("Skip phase poll {} - SourceEventPosition LSN: {}", i, formatLsn(currentLsn));

                    Thread.sleep(200);
                }
                catch (InstanceNotFoundException e) {
                    LOGGER.debug("Metrics not yet available");
                    duringSkipLsns.add(null);
                }
            }

            // STEP 6: Insert new data to verify connector is responsive
            LOGGER.info("=== Inserting new data after restart ===");
            TestHelper.execute("INSERT INTO simple (val) VALUES (99999);");

            waitForAvailableRecords(1, TimeUnit.SECONDS);
            consumeRecords(1);

            TabularData afterNewDataData = (TabularData) mBeanServer.getAttribute(
                    getStreamingMetricsObjectName(),
                    "SourceEventPosition");
            String lsnAfterNewData = getLsnFromTabularData(afterNewDataData);

            LOGGER.info("After new data, SourceEventPosition LSN: {}", formatLsn(lsnAfterNewData));

            // STEP 7: Assertions

            // During the skip phase, SourceEventPosition should show a monitoring blind spot
            // This can manifest as either:
            // 1. null/empty (no position reported) - happens when skipMessage() prevents updates
            // 2. stale LSN (pre-restart value) - happens if last position is retained but not updated
            long staleOrNullCount = duringSkipLsns.stream()
                    .filter(lsn -> lsn == null || lsn.equals(lsnAfterTxA))
                    .count();

            long nullCount = duringSkipLsns.stream()
                    .filter(lsn -> lsn == null)
                    .count();

            long staleCount = duringSkipLsns.stream()
                    .filter(lsn -> lsn != null && lsn.equals(lsnAfterTxA))
                    .count();

            LOGGER.info("During skip phase: {}/{} polls showed monitoring blind spot", staleOrNullCount, duringSkipLsns.size());
            LOGGER.info("  - {} polls returned null (no position)", nullCount);
            LOGGER.info("  - {} polls returned stale LSN ({})", staleCount, formatLsn(lsnAfterTxA));
            LOGGER.info("  All LSNs seen: {}", duringSkipLsns.stream()
                    .map(this::formatLsn)
                    .collect(java.util.stream.Collectors.toList()));

            if (staleOrNullCount > 0) {
                LOGGER.info("✓ CONFIRMED: SourceEventPosition showed monitoring blind spot during skip phase");
                LOGGER.info("  This demonstrates that operators cannot see the connector's catchup progress");
            }
            else {
                LOGGER.warn("✗ SourceEventPosition was observable during skip phase (unexpected)");
            }

            // The critical assertion:
            // At least some polls during skip phase should show null or stale LSN
            assertThat(staleOrNullCount)
                    .as("SourceEventPosition should be null or stale during skip phase " +
                            "because skipMessage() prevents onEvent() from being called. " +
                            "This creates a monitoring blind spot where operators cannot see " +
                            "that the connector is actively catching up.")
                    .isGreaterThan(0);
            // After processing new data, the metric should have updated
            assertThat(lsnAfterNewData)
                    .as("After skip phase completes and new data is processed, " +
                            "SourceEventPosition should update normally")
                    .isNotEqualTo(lsnAfterTxA);

            stopConnector();
        }
    }

    /**
     * Helper method to extract LSN value from JMX TabularData.
     * SourceEventPosition is exposed as TabularData where each row is a CompositeData
     * with "key" and "value" fields.
     */
    private String getLsnFromTabularData(TabularData tabularData) {
        if (tabularData == null || tabularData.isEmpty()) {
            return null;
        }

        // Iterate through TabularData to find the "lsn" key
        for (Object value : tabularData.values()) {
            CompositeData compositeData = (CompositeData) value;
            if ("lsn".equals(compositeData.get("key"))) {
                return (String) compositeData.get("value");
            }
        }
        return null;
    }

    /**
     * Format LSN for logging in consistent PostgreSQL hex format (e.g., "0/2B3FF00").
     * Metrics return LSN as decimal string (e.g., "45350656"), but logs show hex format.
     * This helper provides consistent formatting for easier comparison.
     *
     * @param lsnString LSN as decimal string from metrics, or null
     * @return Formatted LSN in hex format "X/X", or "null" if input is null
     */
    private String formatLsn(String lsnString) {
        if (lsnString == null) {
            return "null";
        }
        try {
            long lsnValue = Long.parseLong(lsnString);
            return Lsn.valueOf(lsnValue).asString();
        }
        catch (NumberFormatException e) {
            // If it's already in hex format or invalid, return as-is
            return lsnString;
        }
    }

}

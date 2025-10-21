/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.junit.TestLogger;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Integration test for offset context and message processing scenarios.
 * Tests various LSN-based filtering scenarios during connector restart with different offset states.
 *
 * @author Pranav Tiwari
 */
public class OffsetContextMessageProcessingIT extends AbstractRecordsProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetContextMessageProcessingIT.class);

    // Separate tables for each test to ensure complete isolation
    private static final String TEST_TABLE_SCENARIO1 = "offset_context_test_scenario1";
    private static final String TEST_TABLE_SCENARIO2 = "offset_context_test_scenario2";
    private static final String TEST_TABLE_SCENARIO3 = "offset_context_test_scenario3";
    private static final String TEST_TABLE_SCENARIO4 = "offset_context_test_scenario4";
    private static final String TEST_TABLE_SCENARIO5 = "offset_context_test_scenario5";

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public TestRule logTestName = new TestLogger(LOGGER);

    private TestConsumer consumer;
    private LogInterceptor logInterceptor;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        // Clean up any existing replication slots to prevent "all slots in use" error
        cleanupReplicationSlots();

        // Create separate test tables for each scenario to ensure complete isolation
        createTestTable(TEST_TABLE_SCENARIO1);
        createTestTable(TEST_TABLE_SCENARIO2);
        createTestTable(TEST_TABLE_SCENARIO3);
        createTestTable(TEST_TABLE_SCENARIO4);
        createTestTable(TEST_TABLE_SCENARIO5);

        // Create publication
        TestHelper.createPublicationForAllTables();

        // Setup log interceptor to monitor message processing behavior
        logInterceptor = new LogInterceptor(PostgresStreamingChangeEventSource.class);
    }

    /**
     * Clean up any existing replication slots to prevent "all slots in use" errors
     */
    private void cleanupReplicationSlots() {
        try {
            TestHelper.execute(
                    "SELECT pg_drop_replication_slot(slot_name) " +
                            "FROM pg_replication_slots " +
                            "WHERE active = false;");
        }
        catch (Exception e) {
            // Ignore errors - slots might not exist
            LOGGER.debug("Failed to cleanup replication slots: {}", e.getMessage());
        }
    }

    /**
     * Create a test table with the given name
     */
    private void createTestTable(String tableName) {
        String ddlStatement = "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS " + tableName + ";" +
                "CREATE TABLE " + tableName + " (" +
                "  pk SERIAL PRIMARY KEY, " +
                "  data VARCHAR(50), " +
                "  batch_id INTEGER, " +
                "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                ");";
        TestHelper.execute(ddlStatement);
        LOGGER.debug("Created test table: {}", tableName);
    }

    @After
    public void after() throws Exception {
        // Always stop connector and clean up slots after each test
        try {
            stopConnector();
        }
        catch (Exception e) {
            // Ignore if connector is already stopped
        }

        // Clean up replication slots after each test
        cleanupReplicationSlots();

        // Clear any remaining consumer records to prevent test interference
        if (consumer != null) {
            try {
                consumer.clear();
            }
            catch (Exception e) {
                // Ignore cleanup errors
            }
        }

        // Small delay to ensure cleanup is complete before next test
        try {
            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Override to use the correct server name for streaming metrics
     */
    @Override
    protected void waitForStreamingToStart() throws InterruptedException {
        // Get the current test method name to determine which server name to use
        String testMethodName = getCurrentTestMethodName();
        String serverName = getServerNameForTest(testMethodName);
        waitForStreamingRunning("postgres", serverName);
    }

    private String getCurrentTestMethodName() {
        return Thread.currentThread().getStackTrace()[3].getMethodName();
    }

    private String getServerNameForTest(String testMethodName) {
        switch (testMethodName) {
            case "shouldProcessAllMessagesWhenOffsetContextIsNull":
                return "test_server_scenario1";
            case "shouldStartFromNextInsertWhenLastProcessedWasCommit":
                return "test_server_scenario2";
            case "shouldResumeFromNextUnprocessedMessageInSameBatch":
                return "test_server_scenario3";
            case "shouldProcessAllMessagesFromIntermediateOffsetWithAdditionalStatements":
                return "test_server_scenario4";
            case "shouldHandleLsnComparisonAcrossMultipleRestartCycles":
                return "test_server_scenario5";
            // Simple message skip optimization test case
            case "testMessageSkipOptimizationOnRestart":
                return "test_server_skip_optimization";
            default:
                return "test_server"; // fallback
        }
    }

    /**
     * Scenario 1: Offset context is null - No message should be skipped
     *
     * When connector starts fresh (no previous offset), all messages should be processed
     * from the beginning of the replication stream.
     */
    @Test
    @FixFor("OFFSET-CONTEXT-SCENARIOS")
    public void shouldProcessAllMessagesWhenOffsetContextIsNull() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true) // Fresh start - no offset context
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SLOT_NAME, "null_offset_context_slot")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("offset.flush.interval.ms", "0")
                .with("offset.flush.timeout.ms", "1000")
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("name", "test-connector-scenario1") // Unique connector name
                .with(PostgresConnectorConfig.TOPIC_PREFIX, "test_server_scenario1") // Unique topic prefix
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public." + TEST_TABLE_SCENARIO1) // Only subscribe to this test's table
                .build();

        start(PostgresConnector.class, config);
        Thread.sleep(2000);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        // Insert test data - all should be processed since no offset context exists
        consumer = testConsumer(5);
        TestHelper.execute(
                "INSERT INTO " + TEST_TABLE_SCENARIO1 + " (data, batch_id) VALUES ('scenario1-msg1', 1);",
                "INSERT INTO " + TEST_TABLE_SCENARIO1 + " (data, batch_id) VALUES ('scenario1-msg2', 1);",
                "INSERT INTO " + TEST_TABLE_SCENARIO1 + " (data, batch_id) VALUES ('scenario1-msg3', 1);",
                "INSERT INTO " + TEST_TABLE_SCENARIO1 + " (data, batch_id) VALUES ('scenario1-msg4', 1);",
                "INSERT INTO " + TEST_TABLE_SCENARIO1 + " (data, batch_id) VALUES ('scenario1-msg5', 1);");

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Verify all messages are processed (no skipping due to null offset context)
        for (int i = 1; i <= 5; i++) {
            SourceRecord record = consumer.remove();
            VerifyRecord.isValidInsert(record, PK_FIELD, i);
            assertThat(((Struct) record.value()).getStruct("after").getString("data")).isEqualTo("scenario1-msg" + i);
            assertThat(((Struct) record.value()).getStruct("after").getInt32("batch_id")).isEqualTo(1);
        }

        LOGGER.info("Scenario 1: Null offset context test completed - all messages processed");
    }

    /**
     * Scenario 2: Offset context exists with last processed message being COMMIT
     *
     * When last processed message was a COMMIT, it means all messages in that transaction
     * were fully processed and flushed. Streaming should start from the next INSERT statement.
     */
    @Test
    @FixFor("OFFSET-CONTEXT-SCENARIOS")
    public void shouldStartFromNextInsertWhenLastProcessedWasCommit() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false) // Preserve offset context
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SLOT_NAME, "commit_offset_context_slot")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("offset.flush.interval.ms", "0")
                .with("offset.flush.timeout.ms", "1000")
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("name", "test-connector-scenario2") // Unique connector name
                .with(PostgresConnectorConfig.TOPIC_PREFIX, "test_server_scenario2") // Unique topic prefix
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public." + TEST_TABLE_SCENARIO2) // Only subscribe to this test's table
                .build();

        // Phase 1: Process initial transaction completely
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        consumer = testConsumer(3);
        TestHelper.execute(
                "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO2 + " (data, batch_id) VALUES ('scenario2-batch1-msg1', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO2 + " (data, batch_id) VALUES ('scenario2-batch1-msg2', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO2 + " (data, batch_id) VALUES ('scenario2-batch1-msg3', 1);" +
                        "COMMIT;" // This COMMIT will be the last processed message
        );

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Consume all records from first transaction
        for (int i = 1; i <= 3; i++) {
            SourceRecord record = consumer.remove();
            VerifyRecord.isValidInsert(record, PK_FIELD, i);
            assertThat(((Struct) record.value()).getStruct("after").getString("data")).isEqualTo("scenario2-batch1-msg" + i);
        }

        stopConnector();
        assertConnectorNotRunning();

        // Phase 2: Insert more data while connector is stopped
        TestHelper.execute(
                "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO2 + " (data, batch_id) VALUES ('scenario2-batch2-msg1', 2);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO2 + " (data, batch_id) VALUES ('scenario2-batch2-msg2', 2);" +
                        "COMMIT;");

        // Phase 3: Restart connector - should start from next INSERT after the committed transaction
        logInterceptor.clear();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        consumer = testConsumer(2);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Verify only new messages are processed (starting from next INSERT after COMMIT)
        SourceRecord record1 = consumer.remove();
        SourceRecord record2 = consumer.remove();

        VerifyRecord.isValidInsert(record1, PK_FIELD, 4);
        VerifyRecord.isValidInsert(record2, PK_FIELD, 5);

        assertThat(((Struct) record1.value()).getStruct("after").getString("data")).isEqualTo("scenario2-batch2-msg1");
        assertThat(((Struct) record2.value()).getStruct("after").getString("data")).isEqualTo("scenario2-batch2-msg2");
        assertThat(((Struct) record1.value()).getStruct("after").getInt32("batch_id")).isEqualTo(2);
        assertThat(((Struct) record2.value()).getStruct("after").getInt32("batch_id")).isEqualTo(2);

        LOGGER.info("Scenario 2: Last processed was COMMIT test completed - resumed from next INSERT");
    }

    /**
     * Scenario 3: Offset context exists with last processed message being intermediate (not COMMIT)
     *
     * PostgreSQL publishes complete transactions only. When a connector processes part of a transaction
     * and stops (with last processed being an intermediate message, not commit), on restart:
     * - PostgreSQL replays the same complete batch (BEGIN -> INSERTs -> COMMIT)
     * - Connector should skip already processed messages and resume from next unprocessed message
     */
    @Test
    @FixFor("OFFSET-CONTEXT-SCENARIOS")
    public void shouldResumeFromNextUnprocessedMessageInSameBatch() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SLOT_NAME, "intermediate_offset_context_slot")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("offset.flush.interval.ms", "0")
                .with("offset.flush.timeout.ms", "1000")
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("name", "test-connector-scenario3") // Unique connector name
                .with(PostgresConnectorConfig.TOPIC_PREFIX, "test_server_scenario3") // Unique topic prefix
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public." + TEST_TABLE_SCENARIO3) // Only subscribe to this test's table
                .build();

        // Phase 1: Process part of a transaction
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        // Execute a complete transaction (PostgreSQL publishes complete transactions only)
        TestHelper.execute(
                "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO3 + " (data, batch_id) VALUES ('scenario3-msg1', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO3 + " (data, batch_id) VALUES ('scenario3-msg2', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO3 + " (data, batch_id) VALUES ('scenario3-msg3', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO3 + " (data, batch_id) VALUES ('scenario3-msg4', 1);" +
                        "COMMIT;");

        // Process only first 2 messages, then stop connector
        consumer = testConsumer(2);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Process only first 2 messages (simulating partial processing of the batch)
        SourceRecord record1 = consumer.remove();
        SourceRecord record2 = consumer.remove();

        VerifyRecord.isValidInsert(record1, PK_FIELD, 1);
        VerifyRecord.isValidInsert(record2, PK_FIELD, 2);

        assertThat(((Struct) record1.value()).getStruct("after").getString("data")).isEqualTo("scenario3-msg1");
        assertThat(((Struct) record2.value()).getStruct("after").getString("data")).isEqualTo("scenario3-msg2");

        // Wait for offset commit (last processed is intermediate message, not commit)
        Thread.sleep(200);
        stopConnector();
        assertConnectorNotRunning();

        // Phase 2: Restart connector
        // PostgreSQL will replay the same complete batch (BEGIN -> INSERT1 -> INSERT2 -> INSERT3 -> INSERT4 -> COMMIT)
        // But connector should skip already processed messages (INSERT1, INSERT2) and resume from INSERT3
        logInterceptor.clear();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        consumer = testConsumer(2);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Should receive only the unprocessed messages (msg3 and msg4) from the same batch
        SourceRecord record3 = consumer.remove();
        SourceRecord record4 = consumer.remove();

        VerifyRecord.isValidInsert(record3, PK_FIELD, 3);
        VerifyRecord.isValidInsert(record4, PK_FIELD, 4);

        assertThat(((Struct) record3.value()).getStruct("after").getString("data")).isEqualTo("scenario3-msg3");
        assertThat(((Struct) record4.value()).getStruct("after").getString("data")).isEqualTo("scenario3-msg4");
        assertThat(((Struct) record3.value()).getStruct("after").getInt32("batch_id")).isEqualTo(1);
        assertThat(((Struct) record4.value()).getStruct("after").getInt32("batch_id")).isEqualTo(1);

        LOGGER.info("Scenario 3: Intermediate message offset test completed - resumed from next unprocessed message in same batch");
    }

    /**
     * Scenario 4: Same as Scenario 3, but with additional SQL statements executed while connector stopped
     *
     * When connector is stopped with intermediate offset, and additional SQL statements are executed:
     * - PostgreSQL replays the incomplete batch, connector skips processed messages and resumes from unprocessed
     * - PostgreSQL also sends all new complete transactions that occurred while connector was stopped
     * - All messages should be processed in correct order without duplicates
     */
    @Test
    @FixFor("OFFSET-CONTEXT-SCENARIOS")
    public void shouldProcessAllMessagesFromIntermediateOffsetWithAdditionalStatements() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SLOT_NAME, "intermediate_additional_slot")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("offset.flush.interval.ms", "0")
                .with("offset.flush.timeout.ms", "1000")
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("name", "test-connector-scenario4") // Unique connector name
                .with(PostgresConnectorConfig.TOPIC_PREFIX, "test_server_scenario4") // Unique topic prefix
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public." + TEST_TABLE_SCENARIO4) // Only subscribe to this test's table
                .build();

        // Phase 1: Process part of a transaction
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        // Execute first complete transaction (PostgreSQL publishes complete transactions only)
        TestHelper.execute(
                "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch1-msg1', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch1-msg2', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch1-msg3', 1);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch1-msg4', 1);" +
                        "COMMIT;");

        // Process only first 3 messages, then stop connector (simulating partial processing)
        consumer = testConsumer(3);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Process first 3 messages (leaving msg4 unprocessed)
        for (int i = 1; i <= 3; i++) {
            SourceRecord record = consumer.remove();
            VerifyRecord.isValidInsert(record, PK_FIELD, i);
            assertThat(((Struct) record.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch1-msg" + i);
        }

        // Wait for offset commit (last processed is intermediate message, not commit)
        Thread.sleep(200);
        stopConnector();
        assertConnectorNotRunning();

        // Phase 2: Add more transactions while connector is stopped
        TestHelper.execute(
                // Add completely new transactions while connector is stopped
                "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch2-msg1', 2);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch2-msg2', 2);" +
                        "COMMIT;" +

                        "BEGIN;" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch3-msg1', 3);" +
                        "INSERT INTO " + TEST_TABLE_SCENARIO4 + " (data, batch_id) VALUES ('scenario4-batch3-msg2', 3);" +
                        "COMMIT;");

        // Phase 3: Restart connector
        // Should process:
        // 1. Remaining message from batch1 (msg4) - PostgreSQL replays complete batch1, skips processed msgs
        // 2. All messages from batch2 and batch3 (new transactions)
        logInterceptor.clear();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingToStart();

        // Should process: batch1-msg4, batch2-msg1, batch2-msg2, batch3-msg1, batch3-msg2
        consumer = testConsumer(5);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        // Verify all remaining messages are processed in correct order
        SourceRecord record4 = consumer.remove(); // batch1-msg4
        SourceRecord record5 = consumer.remove(); // batch2-msg1
        SourceRecord record6 = consumer.remove(); // batch2-msg2
        SourceRecord record7 = consumer.remove(); // batch3-msg1
        SourceRecord record8 = consumer.remove(); // batch3-msg2

        // Verify batch 1 completion
        VerifyRecord.isValidInsert(record4, PK_FIELD, 4);
        assertThat(((Struct) record4.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch1-msg4");
        assertThat(((Struct) record4.value()).getStruct("after").getInt32("batch_id")).isEqualTo(1);

        // Verify batch 2
        VerifyRecord.isValidInsert(record5, PK_FIELD, 5);
        VerifyRecord.isValidInsert(record6, PK_FIELD, 6);
        assertThat(((Struct) record5.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch2-msg1");
        assertThat(((Struct) record6.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch2-msg2");
        assertThat(((Struct) record5.value()).getStruct("after").getInt32("batch_id")).isEqualTo(2);
        assertThat(((Struct) record6.value()).getStruct("after").getInt32("batch_id")).isEqualTo(2);

        // Verify batch 3
        VerifyRecord.isValidInsert(record7, PK_FIELD, 7);
        VerifyRecord.isValidInsert(record8, PK_FIELD, 8);
        assertThat(((Struct) record7.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch3-msg1");
        assertThat(((Struct) record8.value()).getStruct("after").getString("data")).isEqualTo("scenario4-batch3-msg2");
        assertThat(((Struct) record7.value()).getStruct("after").getInt32("batch_id")).isEqualTo(3);
        assertThat(((Struct) record8.value()).getStruct("after").getInt32("batch_id")).isEqualTo(3);

        LOGGER.info("Scenario 4: Intermediate offset with additional statements test completed - all messages processed correctly");
    }

    /**
     * Additional test: Verify LSN comparison logic works correctly across different scenarios
     */
    @Test
    @FixFor("OFFSET-CONTEXT-SCENARIOS")
    public void shouldHandleLsnComparisonAcrossMultipleRestartCycles() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true) // Drop slot to save space
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SLOT_NAME, "lsn_comparison_cycles_slot")
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with("offset.flush.interval.ms", "0")
                .with("offset.flush.timeout.ms", "1000")
                .with(EmbeddedEngineConfig.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("name", "test-connector-scenario5") // Unique connector name
                .with(PostgresConnectorConfig.TOPIC_PREFIX, "test_server_scenario5") // Unique topic prefix
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public." + TEST_TABLE_SCENARIO5) // Only subscribe to this test's table
                .build();

        int totalRecordsProcessed = 0;

        // Perform multiple restart cycles with different data patterns
        for (int cycle = 1; cycle <= 3; cycle++) {
            LOGGER.info("Starting LSN comparison cycle {}", cycle);

            start(PostgresConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingToStart();

            // Insert data for this cycle
            int recordsInCycle = 2 + cycle; // Varying number of records per cycle
            consumer = testConsumer(recordsInCycle);

            StringBuilder insertStatements = new StringBuilder();
            for (int i = 1; i <= recordsInCycle; i++) {
                insertStatements.append("INSERT INTO ").append(TEST_TABLE_SCENARIO5)
                        .append(" (data, batch_id) VALUES ('lsn-cycle").append(cycle)
                        .append("-msg").append(i).append("', ").append(cycle).append(");");
            }
            TestHelper.execute(insertStatements.toString());

            consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

            // Verify records for this cycle
            for (int i = 1; i <= recordsInCycle; i++) {
                SourceRecord record = consumer.remove();
                totalRecordsProcessed++;
                VerifyRecord.isValidInsert(record, PK_FIELD, totalRecordsProcessed);
                assertThat(((Struct) record.value()).getStruct("after").getString("data"))
                        .isEqualTo("lsn-cycle" + cycle + "-msg" + i);
                assertThat(((Struct) record.value()).getStruct("after").getInt32("batch_id")).isEqualTo(cycle);
            }

            // Stop connector between cycles (except last)
            if (cycle < 3) {
                Thread.sleep(200);
                stopConnector();
                assertConnectorNotRunning();
                logInterceptor.clear();
            }
        }

        // Always stop connector after the final cycle
        Thread.sleep(200);
        stopConnector();
        assertConnectorNotRunning();
        logInterceptor.clear();

        // Clean up test data to prevent interference with other tests
        TestHelper.execute("DELETE FROM " + TEST_TABLE_SCENARIO5 + " WHERE data LIKE 'lsn-cycle%';");

        LOGGER.info("LSN comparison across multiple restart cycles completed - {} total records processed correctly", totalRecordsProcessed);
    }
}

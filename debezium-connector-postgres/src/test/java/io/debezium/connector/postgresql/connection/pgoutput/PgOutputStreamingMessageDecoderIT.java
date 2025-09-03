package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.AbstractRecordsProducerTest;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, reason = "Decoding of streaming of inflight messages are present only from 14.")
public class PgOutputStreamingMessageDecoderIT extends AbstractRecordsProducerTest {

    private static final String TOPIC_NAME = "test_server.public.logical_decoding_of_in_flight_transactions";
    private static final String TABLE_NAME = "public.logical_decoding_of_in_flight_transactions";
    private static final long SMALL_TRANSACTION_SIZE = 10;
    private static final long MEDIUM_TRANSACTION_SIZE = 100;
    private static final long LARGE_TRANSACTION_SIZE = 1000;
    private static final long EXTRA_LARGE_TRANSACTION_SIZE = 10000;
    private static final long STREAMING_THRESHOLD_SIZE = 2000; // Size that definitely triggers streaming
    private static final long MASSIVE_STREAMING_SIZE = 5000; // Very large streaming transaction

    @Before
    public void before() throws Exception {
        setupSystemProperties();
        setupDatabase();
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        resetSystemConfiguration();
        cleanupDatabaseState();
    }

    @Test
    public void testSnapshotIsWorking_whenSmallTransactionsHaveExecuted_shouldAbleToProcessAllData() throws InterruptedException {
        String threadName = Thread.currentThread().getName();
        executeInsertSQLStatements(threadName, SMALL_TRANSACTION_SIZE);

        startConnectorWithInitialSnapshot();

        validateRecordsForSingleThread(threadName, SMALL_TRANSACTION_SIZE);
    }

    @Test
    public void testSnapshotIsWorking_whenLargeTransactionsHaveExecuted_shouldAbleToProcessAllData() throws InterruptedException {
        String threadName = Thread.currentThread().getName();
        executeInsertSQLStatements(threadName, LARGE_TRANSACTION_SIZE);

        startConnectorWithInitialSnapshot();

        validateRecordsForSingleThread(threadName, LARGE_TRANSACTION_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenSmallTransactionsAreExecuting_shouldAbleToProcessAllData() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = Thread.currentThread().getName();
        executeInsertSQLStatements(threadName, SMALL_TRANSACTION_SIZE);

        validateRecordsForSingleThread(threadName, SMALL_TRANSACTION_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenLargeInProgressTransactionsAreExecuting_shouldAbleToProcessAllStreamedData() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = Thread.currentThread().getName();
        executeInsertSQLStatements(threadName, LARGE_TRANSACTION_SIZE);

        validateRecordsForSingleThread(threadName, LARGE_TRANSACTION_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenSmallTransactionsAreExecutingAfterSnapshot_shouldAbleToProcessAllData() throws InterruptedException {
        String threadName = Thread.currentThread().getName();

        // Execute transactions before connector start
        executeInsertSQLStatements(threadName, SMALL_TRANSACTION_SIZE);

        startConnectorWithInitialSnapshot();

        // Execute transactions after connector start
        executeInsertSQLStatements(threadName, SMALL_TRANSACTION_SIZE);

        Thread.sleep(5000);

        validateRecordsForSingleThread(threadName, SMALL_TRANSACTION_SIZE * 2);
    }

    @Test
    public void testStreamingIsWorking_whenLargeInProgressTransactionsAreExecutingAfterSnapshot_shouldAbleToProcessAllData() throws InterruptedException {
        String threadName = Thread.currentThread().getName();
        long transactionsBeforeConnector = SMALL_TRANSACTION_SIZE;
        long transactionsAfterConnector = LARGE_TRANSACTION_SIZE;

        executeInsertSQLStatements(threadName, transactionsBeforeConnector);

        startConnectorWithInitialSnapshot();

        executeInsertSQLStatements(threadName, transactionsAfterConnector);

        Thread.sleep(5000);

        validateRecordsForSingleThread(threadName, transactionsBeforeConnector + transactionsAfterConnector);
    }

    @Test
    public void testStreamingIsWorking_whenTwoLargeInProgressTransactionsAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        startConnectorWithInitialSnapshot();

        executeConcurrentTransactions("Thread-1", LARGE_TRANSACTION_SIZE, "Thread-2", LARGE_TRANSACTION_SIZE);

        validateRecordsForMultipleThreads(
                Map.of("Thread-1", LARGE_TRANSACTION_SIZE, "Thread-2", LARGE_TRANSACTION_SIZE));
    }

    @Test
    public void testStreamingIsWorking_whenOneLargeInProgressAndOneSmallTransactionsAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        startConnectorWithInitialSnapshot();

        executeConcurrentTransactions("Thread-1", MEDIUM_TRANSACTION_SIZE, "Thread-2", LARGE_TRANSACTION_SIZE);

        validateRecordsForMultipleThreads(
                Map.of("Thread-1", MEDIUM_TRANSACTION_SIZE, "Thread-2", LARGE_TRANSACTION_SIZE));
    }

    @Test
    public void testStreamingIsWorking_whenOneSmallTransactionsAndOneLargeInProgressAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        startConnectorWithInitialSnapshot();

        executeConcurrentTransactionsWithDelay("Thread-2", EXTRA_LARGE_TRANSACTION_SIZE, "Thread-1", MEDIUM_TRANSACTION_SIZE, 800);

        validateRecordsForMultipleThreads(
                Map.of("Thread-1", MEDIUM_TRANSACTION_SIZE, "Thread-2", EXTRA_LARGE_TRANSACTION_SIZE));
    }

    // ========== Helper Methods ==========

    private void setupSystemProperties() {
        System.setProperty(PostgresConnectorConfig.PLUGIN_NAME.name(), PostgresConnectorConfig.LogicalDecoder.PGOUTPUT.name());
        System.setProperty(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name());
    }

    private void setupDatabase() throws Exception {
        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.executeSystemCommand("ALTER SYSTEM SET logical_decoding_work_mem = '64kB';");
        TestHelper.executeSystemCommand("SELECT pg_reload_conf();");
        TestHelper.execute("DROP TABLE IF EXISTS " + TABLE_NAME + ";",
                "CREATE TABLE " + TABLE_NAME + " (" +
                        " id SERIAL PRIMARY KEY," +
                        " operation_type VARCHAR(10)," +
                        " content TEXT, " +
                        " thread_name TEXT);");
    }

    private void resetSystemConfiguration() throws Exception {
        TestHelper.executeSystemCommand("ALTER SYSTEM SET logical_decoding_work_mem = '64MB';");
        TestHelper.executeSystemCommand("SELECT pg_reload_conf();");
    }

    private void cleanupDatabaseState() throws Exception {
        try {
            // Stop the connector to release resources
            stopConnector();

            // Force rollback any open transactions
            try {
                TestHelper.execute("ROLLBACK;");
            }
            catch (Exception e) {
                // Ignore - no open transaction to rollback
            }

            // Clean up any remaining test data
            TestHelper.execute("TRUNCATE TABLE " + TABLE_NAME + ";");

        }
        catch (Exception e) {
            // Log but don't fail test cleanup
            System.err.println("Warning: Error during database cleanup: " + e.getMessage());
        }
    }

    private void startConnectorWithInitialSnapshot() throws InterruptedException {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    private void executeInsertSQLStatements(String threadName, long count) {
        List<String> sqlStatements = new ArrayList<>();
        for (long i = 1; i <= count; i++) {
            String sql = String.format(
                    "INSERT INTO " + TABLE_NAME + " (operation_type, content, thread_name) " +
                            "VALUES ('insert', 'Record %d from %s', '%s');",
                    i, threadName, threadName);
            sqlStatements.add(sql);
        }

        TestHelper.execute(sqlStatements.get(0), sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
    }

    private void executeConcurrentTransactions(String thread1Name, long thread1Count, String thread2Name, long thread2Count) throws Exception {
        Thread thread1 = createTransactionThread(thread1Name, thread1Count);
        Thread thread2 = createTransactionThread(thread2Name, thread2Count);

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        Thread.sleep(5000);
    }

    private void executeConcurrentTransactionsWithDelay(String thread1Name, long thread1Count, String thread2Name, long thread2Count, long delayMs) throws Exception {
        Thread thread1 = createTransactionThread(thread1Name, thread1Count);
        Thread thread2 = createTransactionThread(thread2Name, thread2Count);

        thread1.start();
        Thread.sleep(delayMs);
        thread2.start();

        thread2.join();
        thread1.join();

        Thread.sleep(5000);
    }

    private Thread createTransactionThread(String threadName, long count) {
        return new Thread(() -> {
            try {
                executeInsertSQLStatements(threadName, count);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void validateRecordsForSingleThread(String expectedThreadName, long expectedCount) throws InterruptedException {
        validateRecordsForMultipleThreads(Map.of(expectedThreadName, expectedCount));
    }

    private void validateRecordsForMultipleThreads(Map<String, Long> expectedThreadCounts) throws InterruptedException {
        int totalExpectedRecords = expectedThreadCounts.values().stream().mapToInt(Long::intValue).sum();

        var actualRecords = consumeRecordsByTopic(totalExpectedRecords);
        var records = actualRecords.recordsForTopic(TOPIC_NAME);

        Assertions.assertThat(records).hasSize(totalExpectedRecords);

        Map<String, Long> recordsByThread = extractRecordsByThread(records);

        expectedThreadCounts.forEach((threadName, expectedCount) -> {
            Assertions.assertThat(recordsByThread.get(threadName))
                    .as("Records count for thread: " + threadName)
                    .isEqualTo(expectedCount);
        });
    }

    private Map<String, Long> extractRecordsByThread(List<SourceRecord> records) {
        return records.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    // ========== Rollback Test Cases ==========

    @Test
    public void testSnapshotIsWorking_whenSmallTransactionIsRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        String threadName = "rollback-small-snapshot";
        executeTransactionWithRollback(threadName, SMALL_TRANSACTION_SIZE);

        startConnectorWithInitialSnapshot();

        // Should have no records since transaction was rolled back
        validateNoRecordsProcessed();
    }

    @Test
    public void testSnapshotIsWorking_whenLargeTransactionIsRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        String threadName = "rollback-large-snapshot";
        executeTransactionWithRollback(threadName, LARGE_TRANSACTION_SIZE);

        startConnectorWithInitialSnapshot();

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenSmallTransactionIsRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "rollback-small-streaming";
        executeTransactionWithRollback(threadName, SMALL_TRANSACTION_SIZE);

        Thread.sleep(2000); // Wait for potential processing
        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenLargeTransactionIsRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "rollback-large-streaming";
        executeTransactionWithRollback(threadName, LARGE_TRANSACTION_SIZE);

        Thread.sleep(2000);
        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenMixedCommitAndRollbackTransactions_onlyCommittedRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String commitThread = "commit-thread";
        String rollbackThread = "rollback-thread";

        // Execute committed transaction
        executeInsertSQLStatements(commitThread, SMALL_TRANSACTION_SIZE);

        // Execute rolled back transaction
        executeTransactionWithRollback(rollbackThread, MEDIUM_TRANSACTION_SIZE);

        Thread.sleep(2000);

        // Should only have records from committed transaction
        validateRecordsForSingleThread(commitThread, SMALL_TRANSACTION_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenConcurrentTransactionsWithOneRollback_onlyCommittedRecordsShouldBeProcessed() throws Exception {
        startConnectorWithInitialSnapshot();

        String commitThread = "concurrent-commit";
        String rollbackThread = "concurrent-rollback";

        Thread commitTx = createTransactionThread(commitThread, MEDIUM_TRANSACTION_SIZE);
        Thread rollbackTx = createRollbackTransactionThread(rollbackThread, MEDIUM_TRANSACTION_SIZE);

        commitTx.start();
        rollbackTx.start();

        commitTx.join();
        rollbackTx.join();

        Thread.sleep(3000);

        // Should only have records from committed transaction
        validateRecordsForSingleThread(commitThread, MEDIUM_TRANSACTION_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenMultipleConcurrentRollbacks_noRecordsShouldBeProcessed() throws Exception {
        startConnectorWithInitialSnapshot();

        String rollbackThread1 = "rollback-thread-1";
        String rollbackThread2 = "rollback-thread-2";

        Thread rollbackTx1 = createRollbackTransactionThread(rollbackThread1, MEDIUM_TRANSACTION_SIZE);
        Thread rollbackTx2 = createRollbackTransactionThread(rollbackThread2, LARGE_TRANSACTION_SIZE);

        rollbackTx1.start();
        rollbackTx2.start();

        rollbackTx1.join();
        rollbackTx2.join();

        Thread.sleep(3000);

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenCommitRollbackCommitSequence_onlyCommittedRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String thread1 = "commit-1";
        String rollbackThread = "rollback-middle";
        String thread2 = "commit-2";

        // First commit
        executeInsertSQLStatements(thread1, SMALL_TRANSACTION_SIZE);

        // Rollback in middle
        executeTransactionWithRollback(rollbackThread, MEDIUM_TRANSACTION_SIZE);

        // Second commit
        executeInsertSQLStatements(thread2, SMALL_TRANSACTION_SIZE);

        Thread.sleep(2000);

        // Should have records from both commits but not rollback
        validateRecordsForMultipleThreads(
                Map.of(thread1, SMALL_TRANSACTION_SIZE, thread2, SMALL_TRANSACTION_SIZE));
    }

    @Test
    public void testStreamingIsWorking_whenLargeTransactionRollbackAfterPartialStreaming_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "large-rollback-streaming";

        // This should test the case where a large transaction starts streaming
        // but then gets rolled back
        executeTransactionWithRollback(threadName, EXTRA_LARGE_TRANSACTION_SIZE);

        Thread.sleep(5000); // Give more time for potential streaming

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenNestedTransactionWithRollback_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "nested-rollback";
        executeNestedTransactionWithRollback(threadName, MEDIUM_TRANSACTION_SIZE);

        Thread.sleep(2000);

        validateNoRecordsProcessed();
    }

    // ========== Large Streaming Transaction Rollback Test Cases ==========

    @Test
    public void testStreamingIsWorking_whenStreamingThresholdTransactionRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "streaming-threshold-rollback";
        executeTransactionWithRollback(threadName, STREAMING_THRESHOLD_SIZE);

        Thread.sleep(2000); // Allow time for streaming processing

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenMassiveStreamingTransactionRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "massive-streaming-rollback";
        executeTransactionWithRollback(threadName, MASSIVE_STREAMING_SIZE);

        Thread.sleep(3000); // Allow time for streaming processing

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenStreamingTransactionRolledBackDuringProcessing_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "streaming-during-processing-rollback";

        // Use a separate thread to ensure we can test rollback during streaming
        Thread streamingRollbackThread = new Thread(() -> {
            try {
                executeTransactionWithRollback(threadName, STREAMING_THRESHOLD_SIZE);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        streamingRollbackThread.start();
        streamingRollbackThread.join();

        Thread.sleep(3000); // Allow time for streaming processing and rollback

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenConcurrentStreamingTransactionsWithRollbacks_noRecordsShouldBeProcessed() throws Exception {
        startConnectorWithInitialSnapshot();

        String rollbackThread1 = "concurrent-streaming-rollback-1";
        String rollbackThread2 = "concurrent-streaming-rollback-2";

        Thread streamingTx1 = createRollbackTransactionThread(rollbackThread1, STREAMING_THRESHOLD_SIZE);
        Thread streamingTx2 = createRollbackTransactionThread(rollbackThread2, MASSIVE_STREAMING_SIZE);

        streamingTx1.start();
        Thread.sleep(1000); // Small delay to stagger the start
        streamingTx2.start();

        streamingTx1.join();
        streamingTx2.join();

        Thread.sleep(3000); // Allow time for streaming processing

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenMixedStreamingTransactionsCommitAndRollback_onlyCommittedRecordsShouldBeProcessed() throws Exception {
        startConnectorWithInitialSnapshot();

        String commitThread = "streaming-commit";
        String rollbackThread = "streaming-rollback";

        // Execute a large streaming transaction that commits
        Thread commitTx = createTransactionThread(commitThread, STREAMING_THRESHOLD_SIZE);

        // Execute a large streaming transaction that rolls back
        Thread rollbackTx = createRollbackTransactionThread(rollbackThread, STREAMING_THRESHOLD_SIZE);

        commitTx.start();
        Thread.sleep(500); // Small stagger
        rollbackTx.start();

        commitTx.join();
        rollbackTx.join();

        Thread.sleep(3000); // Allow time for streaming processing

        // Should only have records from committed streaming transaction
        validateRecordsForSingleThread(commitThread, STREAMING_THRESHOLD_SIZE);
    }

    @Test
    public void testStreamingIsWorking_whenStreamingTransactionPartiallyProcessedThenRolledBack_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "partial-streaming-rollback";

        // This test simulates a scenario where a large transaction starts streaming,
        // some of its data is processed, but then the entire transaction is rolled back
        executeTransactionWithDelayedRollback(threadName, LARGE_TRANSACTION_SIZE, 500);

        Thread.sleep(3000); // Allow time for partial streaming and rollback processing

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenNestedStreamingTransactionWithSavepointRollback_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        String threadName = "nested-streaming-rollback";
        executeNestedStreamingTransactionWithRollback(threadName, LARGE_TRANSACTION_SIZE);

        Thread.sleep(3000); // Allow time for streaming processing

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenMultipleStreamingTransactionsSequentialRollbacks_noRecordsShouldBeProcessed() throws InterruptedException {
        startConnectorWithInitialSnapshot();

        // Execute multiple large streaming transactions in sequence, all rolled back
        String thread1 = "sequential-streaming-rollback-1";
        String thread2 = "sequential-streaming-rollback-2";
        String thread3 = "sequential-streaming-rollback-3";

        executeTransactionWithRollback(thread1, STREAMING_THRESHOLD_SIZE);
        Thread.sleep(3000);

        executeTransactionWithRollback(thread2, MASSIVE_STREAMING_SIZE);
        Thread.sleep(5000);

        executeTransactionWithRollback(thread3, STREAMING_THRESHOLD_SIZE);
        Thread.sleep(3000);

        validateNoRecordsProcessed();
    }

    @Test
    public void testStreamingIsWorking_whenStreamingTransactionInterruptedAndRolledBack_noRecordsShouldBeProcessed() throws Exception {
        startConnectorWithInitialSnapshot();

        String threadName = "interrupted-streaming-rollback";

        // Simulate a streaming transaction that gets interrupted and rolled back
        Thread interruptibleTx = new Thread(() -> {
            try {
                executeTransactionWithInterruptAndRollback(threadName, STREAMING_THRESHOLD_SIZE);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        interruptibleTx.start();
        Thread.sleep(3000); // Let it start streaming

        // Transaction will rollback internally after some processing
        interruptibleTx.join();

        Thread.sleep(3000); // Allow time for cleanup

        validateNoRecordsProcessed();
    }

    // ========== Rollback Helper Methods ==========

    private void executeTransactionWithRollback(String threadName, long count) {
        List<String> sqlStatements = buildInsertStatements(threadName, count);
        sqlStatements.add("ROLLBACK;");

        TestHelper.execute(sqlStatements.get(0),
                sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
    }

    private void executeNestedTransactionWithRollback(String threadName, long count) {
        List<String> sqlStatements = new ArrayList<>();
        sqlStatements.add("BEGIN;");
        sqlStatements.add("SAVEPOINT sp1;");

        // Add insert statements
        sqlStatements.addAll(buildInsertStatements(threadName, count));

        sqlStatements.add("ROLLBACK TO sp1;");
        sqlStatements.add("ROLLBACK;");

        TestHelper.execute(sqlStatements.get(0),
                sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
    }

    private void executeTransactionWithDelayedRollback(String threadName, long count, long delayMs) {
        try {
            List<String> sqlStatements = new ArrayList<>();

            // Add first batch of insert statements
            long halfCount = count / 2;
            sqlStatements.addAll(buildInsertStatements(threadName + "-part1", halfCount));

            // Execute first part and allow some streaming to occur
            try (var connection = TestHelper.executeWithoutCommit(sqlStatements.get(0),
                    sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]))) {

                try {
                    Thread.sleep(delayMs); // Allow partial streaming
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // Add second batch of statements to same transaction
                List<String> secondBatch = buildInsertStatements(threadName + "-part2", count - halfCount);
                for (String stmt : secondBatch) {
                    connection.executeWithoutCommitting(stmt);
                }

                // Rollback the entire transaction
                connection.connection().rollback();
            }
        }
        catch (Exception e) {
            // Ensure rollback happens even if there's an error
            try {
                TestHelper.execute("ROLLBACK;");
            }
            catch (Exception ignored) {
                // Ignore errors during cleanup rollback
            }
            throw new RuntimeException(e);
        }
    }

    private void executeNestedStreamingTransactionWithRollback(String threadName, long count) {
        List<String> sqlStatements = new ArrayList<>();
        sqlStatements.add("BEGIN;");

        // Create multiple savepoints with large data sets
        long partSize = count / 3;

        sqlStatements.add("SAVEPOINT sp1;");
        sqlStatements.addAll(buildInsertStatements(threadName + "-sp1", partSize));

        sqlStatements.add("SAVEPOINT sp2;");
        sqlStatements.addAll(buildInsertStatements(threadName + "-sp2", partSize));

        sqlStatements.add("SAVEPOINT sp3;");
        sqlStatements.addAll(buildInsertStatements(threadName + "-sp3", count - (2 * partSize)));

        // Rollback all savepoints and the transaction
        sqlStatements.add("ROLLBACK TO sp1;");
        sqlStatements.add("ROLLBACK;");

        TestHelper.execute(sqlStatements.get(0),
                sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
    }

    private void executeTransactionWithInterruptAndRollback(String threadName, long count) {
        try {
            // Simulate an interrupted transaction by inserting data in chunks
            long chunkSize = 500; // Reduced chunk size to speed up testing
            long processedCount = 0;

            try (var connection = TestHelper.create()) {
                connection.setAutoCommit(false);

                while (processedCount < count) {
                    long currentChunk = Math.min(chunkSize, count - processedCount);
                    List<String> chunkStatements = buildInsertStatements(
                            threadName + "-chunk" + (processedCount / chunkSize), currentChunk);

                    // Execute chunk in the same transaction
                    for (String stmt : chunkStatements) {
                        connection.executeWithoutCommitting(stmt);
                    }

                    processedCount += currentChunk;

                    // Simulate processing delay (reduced)
                    try {
                        Thread.sleep(50);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    // Simulate interruption after processing some chunks
                    if (processedCount >= count / 2) {
                        break;
                    }
                }

                // Rollback the entire transaction
                connection.connection().rollback();
            }
        }
        catch (Exception e) {
            // Ensure rollback happens even if there's an error
            try {
                TestHelper.execute("ROLLBACK;");
            }
            catch (Exception ignored) {
                // Ignore errors during cleanup rollback
            }
            throw new RuntimeException(e);
        }
    }

    private Thread createRollbackTransactionThread(String threadName, long count) {
        return new Thread(() -> {
            try {
                executeTransactionWithRollback(threadName, count);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<String> buildInsertStatements(String threadName, long count) {
        List<String> sqlStatements = new ArrayList<>();
        for (long i = 1; i <= count; i++) {
            String sql = String.format(
                    "INSERT INTO " + TABLE_NAME + " (operation_type, content, thread_name) " +
                            "VALUES ('insert', 'Record %d from %s', '%s');",
                    i, threadName, threadName);
            sqlStatements.add(sql);
        }
        return sqlStatements;
    }

    private void validateNoRecordsProcessed() throws InterruptedException {
        try {
            // Use a very short timeout to avoid hanging when no records are expected
            var actualRecords = consumeRecordsByTopic(1, false); // Don't assert if no records found
            var records = actualRecords.recordsForTopic(TOPIC_NAME);

            // Handle case where recordsForTopic returns null when no records exist
            if (records == null || records.isEmpty()) {
                // No records found for the topic, which is expected for rollback scenarios
                return;
            }

            // If we reach here, there are unexpected records - this should fail the test
            Assertions.fail("Expected no records for rollback scenario, but found " + records.size() + " records");

        }
        catch (Exception e) {
            // If timeout occurs or no records are available, that's expected for rollback scenarios
            // This is acceptable as it means no records were processed
        }
    }

}
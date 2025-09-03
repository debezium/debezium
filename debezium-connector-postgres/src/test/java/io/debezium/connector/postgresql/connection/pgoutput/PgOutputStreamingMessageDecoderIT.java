package io.debezium.connector.postgresql.connection.pgoutput;

import io.debezium.connector.postgresql.AbstractRecordsProducerTest;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, reason = "Decoding of streaming of inflight messages are present only from 14.")
public class PgOutputStreamingMessageDecoderIT extends AbstractRecordsProducerTest {

    @Before
    public void before() throws Exception {

        System.setProperty(PostgresConnectorConfig.PLUGIN_NAME.name(), PostgresConnectorConfig.LogicalDecoder.PGOUTPUT.name());
        System.setProperty(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name());

        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.executeSystemCommand("ALTER SYSTEM SET logical_decoding_work_mem = '64kB';");
        TestHelper.executeSystemCommand("SELECT pg_reload_conf();");
        TestHelper.execute("DROP TABLE IF EXISTS public.logical_decoding_of_in_flight_transactions;",
                "CREATE TABLE public.logical_decoding_of_in_flight_transactions (" +
                        " id SERIAL PRIMARY KEY," +
                        " operation_type VARCHAR(10)," +
                        " content TEXT, " +
                        " thread_name TEXT);");

        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        TestHelper.executeSystemCommand("ALTER SYSTEM SET logical_decoding_work_mem = '64MB';");
        TestHelper.executeSystemCommand("SELECT pg_reload_conf();");
    }

    @Test
    public void testSnapshotIsWorking_whenSmallTransactionsHaveExecuted_shouldAbleToProcessAllData() throws InterruptedException {
        int numberOfInsertStatements = 10;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        var actualRecords = consumeRecordsByTopic(numberOfInsertStatements);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(numberOfInsertStatements);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(numberOfInsertStatements);
    }

    @Test
    public void testSnapshotIsWorking_whenLargeTransactionsHaveExecuted_shouldAbleToProcessAllData() throws InterruptedException {
        int numberOfInsertStatements = 1000;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        var actualRecords = consumeRecordsByTopic(numberOfInsertStatements);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(numberOfInsertStatements);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(numberOfInsertStatements);
    }

    @Test
    public void testStreamingIsWorking_whenSmallTransactionsAreExecuting_shouldAbleToProcessAllData() throws InterruptedException {

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        int numberOfInsertStatements = 10;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        var actualRecords = consumeRecordsByTopic(numberOfInsertStatements);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(numberOfInsertStatements);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(numberOfInsertStatements);
    }

    @Test
    public void testStreamingIsWorking_whenLargeInProgressTransactionsAreExecuting_shouldAbleToProcessAllStreamedData() throws InterruptedException {

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        int numberOfInsertStatements = 1000;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        var actualRecords = consumeRecordsByTopic(numberOfInsertStatements);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(numberOfInsertStatements);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(numberOfInsertStatements);
    }

    @Test
    public void testStreamingIsWorking_whenSmallTransactionsAreExecutingAfterSnapshot_shouldAbleToProcessAllData() throws InterruptedException {

        int numberOfInsertStatements = 10;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfInsertStatements);

        Thread.sleep(5000);

        var actualRecords = consumeRecordsByTopic(numberOfInsertStatements * 2);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(numberOfInsertStatements * 2);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(numberOfInsertStatements * 2);
    }

    @Test
    public void testStreamingIsWorking_whenLargeInProgressTransactionsAreExecutingAfterSnapshot_shouldAbleToProcessAllData() throws InterruptedException {

        int numberOfTransactionBeforeStartOfConnector = 10;
        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfTransactionBeforeStartOfConnector);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        int numberOfTransactionAfterStartOfConnector = 1000;

        executeInsertSQLStatements(Thread.currentThread().getName(), numberOfTransactionAfterStartOfConnector);

        int totalTransactions = numberOfTransactionBeforeStartOfConnector + numberOfTransactionAfterStartOfConnector;

        Thread.sleep(5000);

        var actualRecords = consumeRecordsByTopic(totalTransactions);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
        Assertions.assertThat(recs).hasSize(totalTransactions);

        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get(Thread.currentThread().getName())).isEqualTo(totalTransactions);
    }

    @Test
    public void testStreamingIsWorking_whenTwoLargeInProgressTransactionsAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        final int numRecords = 1000;

        // Create two concurrent tasks
        Thread thread1 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-1", numRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-2", numRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Start both transactions concurrently
        thread1.start();
        thread2.start();

        // Wait for both to finish
        thread1.join();
        thread2.join();

        Thread.sleep(5000);

        // Total expected records = 500 + 500
        var totalRecords = numRecords * 2;
        var actualRecords = consumeRecordsByTopic(totalRecords);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");

        Assertions.assertThat(recs).hasSize(totalRecords);

        // Group and validate by thread name
        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get("Thread-1")).isEqualTo(numRecords);
        Assertions.assertThat(recordsByThread.get("Thread-2")).isEqualTo(numRecords);
    }

    @Test
    public void testStreamingIsWorking_whenOneLargeInProgressAndOneSmallTransactionsAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        final int numberOfSmallRecords = 100;

        // Create two concurrent tasks
        Thread thread1 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-1", numberOfSmallRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final int numberOfLargeRecords = 1000;

        Thread thread2 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-2", numberOfLargeRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Start both transactions concurrently
        thread1.start();
        thread2.start();

        // Wait for both to finish
        thread1.join();
        thread2.join();

        Thread.sleep(5000);

        // Total expected records = 500 + 500
        var totalRecords = numberOfSmallRecords + numberOfLargeRecords;
        var actualRecords = consumeRecordsByTopic(totalRecords);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");

        Assertions.assertThat(recs).hasSize(totalRecords);

        // Group and validate by thread name
        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get("Thread-1")).isEqualTo(numberOfSmallRecords);
        Assertions.assertThat(recordsByThread.get("Thread-2")).isEqualTo(numberOfLargeRecords);
    }

    @Test
    public void testStreamingIsWorking_whenOneSmallTransactionsAndOneLargeInProgressAreExecutingConcurrently_shouldAbleToProcessAllData() throws Exception {
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        final int numberOfLargeRecords = 10000;

        Thread thread2 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-2", numberOfLargeRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final int numberOfSmallRecords = 100;

        // Create two concurrent tasks
        Thread thread1 = new Thread(() -> {
            try {
                executeInsertSQLStatements("Thread-1", numberOfSmallRecords);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Start both transactions concurrently
        thread2.start();
        Thread.sleep(800);
        thread1.start();

        // Wait for both to finish
        thread1.join();
        thread2.join();

        Thread.sleep(5000);

        // Total expected records = 500 + 500
        var totalRecords = numberOfSmallRecords + numberOfLargeRecords;
        var actualRecords = consumeRecordsByTopic(totalRecords);
        var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");

        Assertions.assertThat(recs).hasSize(totalRecords);

        // Group and validate by thread name
        Map<String, Long> recordsByThread = recs.stream()
                .map(r -> ((Struct) r.value()).getStruct("after").getString("thread_name"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Assertions.assertThat(recordsByThread.get("Thread-1")).isEqualTo(numberOfSmallRecords);
        Assertions.assertThat(recordsByThread.get("Thread-2")).isEqualTo(numberOfLargeRecords);
    }

    private void executeInsertSQLStatements(String threadName, int count) {

        List<String> sqlStatements = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            String sql = String.format(
                    "INSERT INTO public.logical_decoding_of_in_flight_transactions (operation_type, content, thread_name) " +
                            "VALUES ('insert', 'Record %d from %s', '%s');",
                    i, threadName, threadName);
            sqlStatements.add(sql);
        }

        TestHelper.execute(sqlStatements.get(0), sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
    }

    /*
     * @Test
     * public void whenReplicationModeIsStreaming_withBeginAndRollBack_noTransactionShouldBeProcessed() throws InterruptedException {
     * start(PostgresConnector.class, TestHelper.defaultConfig()
     * .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
     * .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
     * .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
     * .build());
     * assertConnectorIsRunning();
     *
     * waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
     * List<String> sqlStatements = new ArrayList<>();
     * int i = 1;
     * for (; i<=2; i++) {
     * String sql = String.format(
     * "INSERT INTO public.logical_decoding_of_in_flight_transactions (operation_type, content, thread_name) " +
     * "VALUES ('%s', '%s', '%s');",
     * "insert", "New user record created at index = " + i, Thread.currentThread().getName()
     * );
     * sqlStatements.add(sql);
     * }
     *
     * sqlStatements.add("ROLLBACK;");
     * TestHelper.execute(sqlStatements.get(0), sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
     *
     * var actualRecords = consumeRecordsByTopic(i);
     * var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
     * Assertions.assertThat(recs).hasSize(1);
     * }
     *
     * @Test
     * public void whenReplicationModeIsStreaming_withBeginAndRollBackForLargeTransactions_noTransactionShouldBeProcessed() throws InterruptedException {
     * start(PostgresConnector.class, TestHelper.defaultConfig()
     * .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
     * .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
     * .with(PostgresConnectorConfig.LOGICAL_REPLICATION_MODE.name(), PostgresConnectorConfig.LogicalReplicationMode.STREAMING.name())
     * .build());
     * assertConnectorIsRunning();
     *
     * waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
     * List<String> sqlStatements = new ArrayList<>();
     * int i = 1;
     * for (; i<=500; i++) {
     * String sql = String.format(
     * "INSERT INTO public.logical_decoding_of_in_flight_transactions (operation_type, content, thread_name) " +
     * "VALUES ('%s', '%s', '%s');",
     * "insert", "New user record created at index = " + i, Thread.currentThread().getName()
     * );
     * sqlStatements.add(sql);
     * }
     *
     * sqlStatements.add("ROLLBACK;");
     * TestHelper.execute(sqlStatements.get(0), sqlStatements.subList(1, sqlStatements.size()).toArray(new String[0]));
     *
     * var actualRecords = consumeRecordsByTopic(i);
     * var recs = actualRecords.recordsForTopic("test_server.public.logical_decoding_of_in_flight_transactions");
     * Assertions.assertThat(recs).hasSize(1);
     * }
     */
}

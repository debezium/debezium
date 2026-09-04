/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.util.Collect;

@SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
public class TransactionMetadataIT extends AbstractAsyncEngineConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));" +
            INSERT_STMT;

    @BeforeAll
    static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    void transactionMetadata() throws InterruptedException {
        // Testing.Print.enable();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDefaultReplicationSlotBeActive();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);

        // BEGIN, 2 * data, END
        final List<SourceRecord> records = new ArrayList<>();

        // Database sometimes insert an empty transaction, we must skip those
        Awaitility.await("Skip empty transactions and find the data").atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords() * 3L)).until(() -> {
            final List<SourceRecord> candidate = consumeRecordsByTopic(2).allRecordsInOrder();
            if (candidate.get(1).topic().contains("transaction")) {
                // empty transaction, should be skipped
                return false;
            }
            records.addAll(candidate);
            records.addAll(consumeRecordsByTopic(2).allRecordsInOrder());
            return true;
        });

        assertThat(records).hasSize(4);
        final String beginTxId = assertBeginTransaction(records.get(0));
        assertRecordTransactionMetadata(records.get(1), beginTxId, 1, 1);
        assertRecordTransactionMetadata(records.get(2), beginTxId, 2, 1);
        assertEndTransaction(records.get(3), beginTxId, 2, Collect.hashMapOf("s1.a", 1, "s2.a", 1));
    }

    @Test
    @FixFor("debezium/dbz#633")
    public void shouldContinueTransactionEventCountersAfterRestartInMiddleOfTransaction() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        final Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA.getValue())
                // Keep the replication slot and the offsets across the restart so that the interrupted
                // transaction is resumed and replayed from its beginning instead of being dropped
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                // Keep the engine batches small so that stopping the connector cannot deliver records
                // past the middle of the transaction before the stop takes effect
                .with(PostgresConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // Write a single transaction interleaving two tables that is much larger than the record queue
        // of the test framework, so the connector is stopped before the whole transaction is delivered
        final int insertsPerTable = 150;
        try (PostgresConnection connection = TestHelper.create()) {
            connection.setAutoCommit(false);
            final String[] inserts = new String[2 * insertsPerTable];
            for (int i = 0; i < insertsPerTable; i++) {
                inserts[2 * i] = "INSERT INTO s1.a (aa) VALUES (" + i + ")";
                inserts[2 * i + 1] = "INSERT INTO s2.a (aa) VALUES (" + i + ")";
            }
            connection.executeWithoutCommitting(inserts);
            connection.commit();
        }

        // Consume the beginning of the transaction, then stop the connector while the rest of the
        // transaction is still being delivered
        final List<SourceRecord> records = new ArrayList<>(consumeRecordsByTopic(51).allRecordsInOrder());
        final String txId = findTransactionId(records);
        assertThat(txId).as("Failed to find the transaction id").isNotNull();
        stopConnector();
        assertConnectorNotRunning();

        // The connector has to stop in the middle of the transaction, i.e. the last committed offset
        // still references the in-progress transaction id (it is cleared once a transaction ends)
        final Map<String, Object> committedOffset = readLastCommittedOffset(config, records.get(0).sourcePartition());
        assertThat(committedOffset.get("transaction_id"))
                .as("Connector has to stop in the middle of the transaction")
                .isEqualTo(txId);

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        // Consume the rest of the transaction including its END event
        for (int i = 0; findTransactionStatusRecords(records, txId, "END").isEmpty() && i < 30; i++) {
            records.addAll(consumeRecordsByTopic(100).allRecordsInOrder());
        }

        // Only a single BEGIN must be emitted for the transaction, the transaction start replayed
        // after the restart must not be propagated
        final List<SourceRecord> beginRecords = findTransactionStatusRecords(records, txId, "BEGIN");
        assertThat(beginRecords).as("Duplicate BEGIN event emitted after restart").hasSize(1);
        assertBeginTransaction(beginRecords.get(0));

        // Event counters must continue after the restart instead of being reset
        assertDataCollectionOrderContinues(records, TestHelper.topicName("s1.a"), txId, insertsPerTable);
        assertDataCollectionOrderContinues(records, TestHelper.topicName("s2.a"), txId, insertsPerTable);

        final List<SourceRecord> endRecords = findTransactionStatusRecords(records, txId, "END");
        assertThat(endRecords).as("Failed to find the transaction END event").isNotEmpty();
        assertEndTransaction(endRecords.get(0), txId, 2 * insertsPerTable,
                Collect.hashMapOf("s1.a", insertsPerTable, "s2.a", insertsPerTable));
    }

    /**
     * Finds the numeric transaction id of the first data change record. Postgres transaction ids used in the
     * transaction metadata are of the form {@code <txid>:<lsn>}; the numeric {@code <txid>} part is stable across
     * a restart while the {@code <lsn>} part differs between the BEGIN, data and END events of a transaction.
     */
    private String findTransactionId(List<SourceRecord> records) {
        return records.stream()
                .filter(record -> record.topic().equals(TestHelper.topicName("s1.a"))
                        || record.topic().equals(TestHelper.topicName("s2.a")))
                .map(record -> ((Struct) record.value()).getStruct("transaction").getString("id"))
                .map(TransactionMetadataIT::toTransactionNumber)
                .findFirst()
                .orElse(null);
    }

    private List<SourceRecord> findTransactionStatusRecords(List<SourceRecord> records, String txId, String status) {
        final String transactionTopic = TestHelper.topicName("transaction");
        final List<SourceRecord> statusRecords = new ArrayList<>();
        for (SourceRecord record : records) {
            if (transactionTopic.equals(record.topic())) {
                final Struct value = (Struct) record.value();
                if (status.equals(value.getString("status")) && txId.equals(toTransactionNumber(value.getString("id")))) {
                    statusRecords.add(record);
                }
            }
        }
        return statusRecords;
    }

    private void assertDataCollectionOrderContinues(List<SourceRecord> records, String topic, String txId, int expectedLastOrder) {
        long previousOrder = 0;
        long lastOrder = 0;
        for (SourceRecord record : records) {
            if (!topic.equals(record.topic())) {
                continue;
            }
            final Struct transaction = ((Struct) record.value()).getStruct("transaction");
            assertThat(toTransactionNumber(transaction.getString("id"))).isEqualTo(txId);
            final long order = transaction.getInt64("data_collection_order");
            assertThat(order)
                    .as("data_collection_order was reset within transaction %s on topic %s", txId, topic)
                    .isGreaterThanOrEqualTo(previousOrder);
            previousOrder = order;
            lastOrder = order;
        }
        assertThat(lastOrder).as("Unexpected last data_collection_order for %s", topic).isEqualTo((long) expectedLastOrder);
    }

    private static String toTransactionNumber(String transactionId) {
        return transactionId == null ? null : transactionId.split(":")[0];
    }

    @Override
    protected String assertBeginTransaction(SourceRecord record) {
        final Struct begin = (Struct) record.value();
        final Struct beginKey = (Struct) record.key();
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

        assertThat(begin.getString("status")).isEqualTo("BEGIN");
        assertThat(begin.getInt64("event_count")).isNull();
        final String txId = begin.getString("id");
        assertThat(beginKey.getString("id")).isEqualTo(txId);

        final String expectedId = Arrays.stream(txId.split(":")).findFirst().get();
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
        return txId;
    }

    @Override
    protected void assertEndTransaction(SourceRecord record, String beginTxId, long expectedEventCount, Map<String, Number> expectedPerTableCount) {
        final Struct end = (Struct) record.value();
        final Struct endKey = (Struct) record.key();
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        final String expectedId = Arrays.stream(beginTxId.split(":")).findFirst().get();
        final String expectedTxId = String.format("%s:%s", expectedId, offset.get("lsn"));

        assertThat(end.getString("status")).isEqualTo("END");
        assertThat(end.getString("id")).isEqualTo(expectedTxId);
        assertThat(end.getInt64("event_count")).isEqualTo(expectedEventCount);
        assertThat(endKey.getString("id")).isEqualTo(expectedTxId);

        assertThat(end.getArray("data_collections").stream().map(x -> (Struct) x)
                .collect(Collectors.toMap(x -> x.getString("data_collection"), x -> x.getInt64("event_count"))))
                .isEqualTo(expectedPerTableCount.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().longValue())));
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
    }

    @Override
    protected void assertRecordTransactionMetadata(SourceRecord record, String beginTxId, long expectedTotalOrder, long expectedCollectionOrder) {
        final Struct change = ((Struct) record.value()).getStruct("transaction");
        final Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        final String expectedId = Arrays.stream(beginTxId.split(":")).findFirst().get();
        final String expectedTxId = String.format("%s:%s", expectedId, offset.get("lsn"));

        assertThat(change.getString("id")).isEqualTo(expectedTxId);
        assertThat(change.getInt64("total_order")).isEqualTo(expectedTotalOrder);
        assertThat(change.getInt64("data_collection_order")).isEqualTo(expectedCollectionOrder);
        assertThat(offset.get("transaction_id")).isEqualTo(expectedId);
    }
}

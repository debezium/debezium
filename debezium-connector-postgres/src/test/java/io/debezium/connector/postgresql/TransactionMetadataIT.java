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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenKafkaVersion;
import io.debezium.junit.SkipWhenKafkaVersion.KafkaVersion;
import io.debezium.util.Collect;

@SkipWhenKafkaVersion(check = EqualityCheck.EQUAL, value = KafkaVersion.KAFKA_1XX, description = "Not compatible with Kafka 1.x")
public class TransactionMetadataIT extends AbstractConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));" +
            INSERT_STMT;

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    public void transactionMetadata() throws InterruptedException {
        // Testing.Print.enable();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
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
import io.debezium.util.Collect;

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
        Awaitility.await("Skip empty transactions and find the data").atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords() * 3)).until(() -> {
            final List<SourceRecord> candidate = consumeRecordsByTopic(2).allRecordsInOrder();
            if (candidate.get(1).topic().contains("transaction")) {
                // empty transaction, should be skipped
                return false;
            }
            records.addAll(candidate);
            records.addAll(consumeRecordsByTopic(2).allRecordsInOrder());
            return true;
        });

        Assertions.assertThat(records).hasSize(4);
        final String txId = assertBeginTransaction(records.get(0));
        assertRecordTransactionMetadata(records.get(1), txId, 1, 1);
        assertRecordTransactionMetadata(records.get(2), txId, 2, 1);
        assertEndTransaction(records.get(3), txId, 2, Collect.hashMapOf("s1.a", 1, "s2.a", 1));
    }
}

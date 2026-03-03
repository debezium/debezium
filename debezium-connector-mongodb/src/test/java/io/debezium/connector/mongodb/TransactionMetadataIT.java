/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Test;

import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Collect;

/**
 * Transaction metadata integration test for Debezium MongoDB connector.
 *
 * @author Chris Cranford
 */
public class TransactionMetadataIT extends AbstractMongoConnectorIT {

    @Test
    public void transactionMetadata() throws Exception {
        // Testing.Print.enable();
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        if (!TestHelper.transactionsSupported()) {
            return;
        }

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        insertDocumentsInTx("dbA", "c1", documentsToInsert.toArray(new Document[0]));

        List<Document> documentsToInsert2 = loadTestDocuments("restaurants6.json");
        insertDocuments("dbA", "c1", documentsToInsert2.toArray(new Document[0]));

        // BEGIN, data, END, data for change stream
        final SourceRecords records = consumeRecordsByTopic(1 + 6 + 1 + 1);
        final List<SourceRecord> c1s = records.recordsForTopic("mongo1.dbA.c1");
        final List<SourceRecord> txs = records.recordsForTopic("mongo1.transaction");
        assertThat(c1s).hasSize(7);
        assertThat(txs).hasSize(2);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId1 = assertBeginTransaction(all.get(0));

        long counter = 1;
        for (int i = 1; i <= 6; ++i) {
            assertRecordTransactionMetadata(all.get(i), txId1, counter, counter);
            counter++;
        }

        assertEndTransaction(all.get(7), txId1, 6, Collect.hashMapOf("dbA.c1", 6));

        stopConnector();
    }

    @Test
    @FixFor("DBZ-4077")
    public void transactionMetadataWithCustomTopicName() throws Exception {
        // Testing.Print.enable();
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(AbstractTopicNamingStrategy.TOPIC_TRANSACTION, "tx.of.server")
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        if (!TestHelper.transactionsSupported()) {
            return;
        }

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        insertDocumentsInTx("dbA", "c1", documentsToInsert.toArray(new Document[0]));

        List<Document> documentsToInsert2 = loadTestDocuments("restaurants6.json");
        insertDocuments("dbA", "c1", documentsToInsert2.toArray(new Document[0]));

        // BEGIN, data, END, data
        final SourceRecords records = consumeRecordsByTopic(1 + 6 + 1 + 1);
        final List<SourceRecord> c1s = records.recordsForTopic("mongo1.dbA.c1");
        final List<SourceRecord> txs = records.recordsForTopic("mongo1.tx.of.server");
        assertThat(c1s).hasSize(7);
        assertThat(txs).hasSize(2);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId1 = assertBeginTransaction(all.get(0));

        long counter = 1;
        for (int i = 1; i <= 6; ++i) {
            assertRecordTransactionMetadata(all.get(i), txId1, counter, counter);
            counter++;
        }

        assertEndTransaction(all.get(7), txId1, 6, Collect.hashMapOf("dbA.c1", 6));

        stopConnector();
    }

    @Test
    public void shouldNotEmitDuplicateEndRecordsForMultipleNonTransactionalEvents() throws Exception {
        // Testing.Print.enable();
        config = TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(mongo, "dbA");

        if (!TestHelper.transactionsSupported()) {
            return;
        }

        start(MongoDbConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        waitForSnapshotToBeCompleted("mongodb", "mongo1");

        // Insert 2 documents in a transaction
        insertDocumentsInTx("dbA", "c1",
                new Document("_id", 1).append("data", "txn-doc-1"),
                new Document("_id", 2).append("data", "txn-doc-2"));

        // Insert first non-transactional document - this should trigger END
        insertDocuments("dbA", "c1", new Document("_id", 3).append("data", "non-txn-doc-1"));

        // Insert second non-transactional document - this should NOT trigger another END
        insertDocuments("dbA", "c1", new Document("_id", 4).append("data", "non-txn-doc-2"));

        // Insert third non-transactional document - this should NOT trigger another END
        insertDocuments("dbA", "c1", new Document("_id", 5).append("data", "non-txn-doc-3"));

        // Expected records:
        // - BEGIN (1 transaction record)
        // - 2 data events from transaction
        // - END (1 transaction record) - triggered by first non-txn event
        // - 3 data events from non-transactional inserts
        // Total: 1 + 2 + 1 + 3 = 7 records, with exactly 2 transaction records
        final SourceRecords records = consumeRecordsByTopic(1 + 2 + 1 + 3);
        final List<SourceRecord> dataRecords = records.recordsForTopic("mongo1.dbA.c1");
        final List<SourceRecord> txRecords = records.recordsForTopic("mongo1.transaction");

        // Verify we have exactly 5 data records (2 from txn + 3 non-txn)
        assertThat(dataRecords).hasSize(5);

        // Critical assertion: we should have exactly 2 transaction records (1 BEGIN + 1 END)
        // Before the fix, this would fail with 4 records (1 BEGIN + 3 ENDs)
        assertThat(txRecords).hasSize(2);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId = assertBeginTransaction(all.get(0));
        assertEndTransaction(all.get(3), txId, 2, Collect.hashMapOf("dbA.c1", 2));

        stopConnector();
    }
}

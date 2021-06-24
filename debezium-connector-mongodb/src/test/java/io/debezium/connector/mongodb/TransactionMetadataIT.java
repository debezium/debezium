/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Test;

import io.debezium.connector.mongodb.MongoDbConnectorConfig.SnapshotMode;
import io.debezium.util.Collect;

/**
 * Transaction metadata integration test for Debezium MongoDB connector.
 *
 * @author Chris Cranford
 */
public class TransactionMetadataIT extends AbstractMongoConnectorIT {

    @Test
    public void transactionMetadata() throws Exception {
        config = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbA.c1")
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(MongoDbConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        context = new MongoDbTaskContext(config);

        TestHelper.cleanDatabase(primary(), "dbA");

        if (!TestHelper.transactionsSupported(primary(), "mongo1")) {
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

        // BEGIN, data, END, BEGIN data
        final SourceRecords records = consumeRecordsByTopic(1 + 6 + 1 + 1 + 1);
        final List<SourceRecord> c1s = records.recordsForTopic("mongo1.dbA.c1");
        final List<SourceRecord> txs = records.recordsForTopic("mongo1.transaction");
        assertThat(c1s).hasSize(7);
        assertThat(txs).hasSize(3);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId1 = assertBeginTransaction(all.get(0));

        long counter = 1;
        for (int i = 1; i <= 6; ++i) {
            assertRecordTransactionMetadata(all.get(i), txId1, counter, counter);
            counter++;
        }

        assertEndTransaction(all.get(7), txId1, 6, Collect.hashMapOf("rs0.dbA.c1", 6));

        final String txId2 = assertBeginTransaction(all.get(8));
        assertRecordTransactionMetadata(all.get(9), txId2, 1, 1);

        stopConnector();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mongodb.converters.MongoDbRecordParser;
import io.debezium.util.Testing;

public class MongoDbChangestreamTransactionIT extends AbstractMongoConnectorIT {

    private static final String mongodb = "mongo";
    private static final String db = "mongdbtxn";
    private static final String col = "collection1";

    @Before
    public void beforeEach() {
        super.beforeEach();

        Testing.Print.enable();
        // Testing.Debug.enable();

        config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.LOGICAL_NAME, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_with_pre_image")
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(primary(), db);

        if (!TestHelper.transactionsSupported(primary(), mongodb)) {
            return;
        }

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", mongodb);
        assertConnectorIsRunning();
    }

    /**
     * Verifies that all transactional events should also have indices in source
     * Events in the same transaction should have the same timestamp. TXN_INDEX should be monotonically increasing
     * This also tests the recordParser
     *
     * @throws Exception if test fails
     */
    @Test
    public void transactionEventsSourceShouldHaveTXN_INDEX() throws Exception {
        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        Document[] docs = documentsToInsert.toArray(new Document[0]);
        insertDocumentsInTx(db, col, docs);

        final SourceRecords records = consumeRecordsByTopic(docs.length);
        assertNoRecordsToConsume();

        BsonTimestamp ts = null;
        long prevTxnIdx = 0;
        for (int i = 0; i < docs.length; i++) {
            SourceRecord record = records.allRecordsInOrder().get(i);
            MongoDbRecordParser recordParser = new MongoDbRecordParser(record.valueSchema(), (Struct) record.value());

            // set if unset
            if (ts == null) {
                ts = new BsonTimestamp(((Long) recordParser.getMetadata(SourceInfo.TIMESTAMP_KEY)).intValue() / 1000, (int) recordParser.getMetadata(SourceInfo.ORDER));
            }

            // assert that timestamps are equal for all events
            assertThat(ts).isEqualTo(
                    new BsonTimestamp(((Long) recordParser.getMetadata(SourceInfo.TIMESTAMP_KEY)).intValue() / 1000, (int) recordParser.getMetadata(SourceInfo.ORDER)));
            // assert that current = previous + 1;
            long currTxnIdx = (long) recordParser.getMetadata(SourceInfo.TXN_INDEX);
            assertThat(prevTxnIdx + 1).isEqualTo(currTxnIdx);
            prevTxnIdx = currTxnIdx;
        }
    }
}

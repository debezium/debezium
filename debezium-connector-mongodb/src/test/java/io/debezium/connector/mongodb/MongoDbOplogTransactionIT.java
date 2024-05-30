/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.connector.mongodb.MongoDbFieldName.RAW_OPLOG_FIELD;
import static io.debezium.connector.mongodb.MongoDbFieldName.TIMESTAMP;
import static io.debezium.connector.mongodb.MongoDbFieldName.TXN_INDEX;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;

import io.debezium.config.Field;
import io.debezium.connector.mongodb.converters.MongoDbRecordParser;
import io.debezium.util.Testing;

public class MongoDbOplogTransactionIT extends AbstractMongoConnectorIT {
    private static final String mongodb = "mongo";
    private static final String db = "mongdbtxn";
    private static final String col = "collection1";

    @Before
    public void beforeEach() {
        Testing.Print.enable();
        beforeEach(List.of(), List.of());
    }

    /*
     * helper that allows tests to override/update default configuration
     */
    private void beforeEach(List<Field> configFields, List<Object> configValues) {
        // call superclass's beforeEach
        super.beforeEach();

        var configBuilder = TestHelper.getConfiguration()
                .edit()
                .with(MongoDbConnectorConfig.LOGICAL_NAME, mongodb)
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, MongoDbConnectorConfig.CaptureMode.OPLOG)
                .with(MongoDbConnectorConfig.RAW_OPLOG_ENABLED, true);

        // field and value lengths should be the same
        assertThat(configFields.size()).isEqualTo(configValues.size());

        // add additional config fields (may override others)
        for (int i = 0; i < configFields.size(); i++) {
            configBuilder.with(configFields.get(i), configValues.get(i));
        }
        config = configBuilder.build();

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
     * Verifies that all transactional events should also have indices.
     * Events in the same transaction should have the same timestamp.
     *
     * @throws Exception if test fails
     */
    @Test
    public void transactionEventsShouldHaveTimestampsAndIndices() throws Exception {

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        Document[] docs = documentsToInsert.toArray(new Document[0]);
        insertDocumentsInTx(db, col, docs);

        final SourceRecords records = consumeRecordsByTopic(docs.length);
        assertNoRecordsToConsume();

        BsonTimestamp ts = null;
        for (int i = 0; i < docs.length; i++) {
            SourceRecord record = records.allRecordsInOrder().get(i);
            RawBsonDocument oplog = new RawBsonDocument(((Struct) record.value()).getBytes(RAW_OPLOG_FIELD));
            Testing.print(oplog);
            assertThat(oplog.containsKey(TIMESTAMP));
            if (ts == null) {
                ts = oplog.getTimestamp(TIMESTAMP);
            }
            else {
                assertThat(oplog.getTimestamp(TIMESTAMP)).isEqualTo(ts);
            }
            assertThat(oplog.containsKey(TXN_INDEX));
            assertThat(oplog.getInt32(TXN_INDEX).intValue()).isEqualTo(i + 1);
        }
    }

    /**
     * The opposite of {@link #transactionEventsShouldHaveTimestampsAndIndices()},
     * Verifies that all non-transactional events have timestamps, and they should all be unique.
     *
     * @throws Exception if test fails
     */
    @Test
    public void nonTransactionEventsShouldHaveTimestamps() throws Exception {

        List<Document> documentsToInsert = loadTestDocuments("restaurants1.json");
        Document[] docs = documentsToInsert.toArray(new Document[0]);
        insertDocuments(db, col, docs);

        final SourceRecords records = consumeRecordsByTopic(docs.length);
        assertNoRecordsToConsume();

        Set<BsonTimestamp> timestamps = new HashSet<>();
        for (int i = 0; i < docs.length; i++) {
            SourceRecord record = records.allRecordsInOrder().get(i);
            RawBsonDocument oplog = new RawBsonDocument(((Struct) record.value()).getBytes(RAW_OPLOG_FIELD));
            Testing.print(oplog);
            assertThat(oplog.containsKey(TIMESTAMP));
            timestamps.add(oplog.getTimestamp(TIMESTAMP));
            assertThat(oplog.containsKey(TXN_INDEX)).isFalse();
        }
        assertThat(timestamps.size()).isEqualTo(docs.length);
    }

    /**
     * Verifies that transactional events in different sessions should have different timestamps.
     *
     * @throws Exception if test fails
     */
    @Test
    public void transactionEventsInDifferentSessionsShouldHaveDifferentTimestamps() throws Exception {

        ObjectId objId1 = new ObjectId();
        Document obj1 = new Document("_id", objId1).append("hello", "testing this");
        ObjectId objId2 = new ObjectId();
        Document obj2 = new Document("_id", objId2).append("hello", "testing this");

        applyOpsInTx(
                (collection, session) -> {
                    collection.insertOne(session, obj1, new InsertOneOptions().bypassDocumentValidation(true));
                    collection.insertOne(session, obj2, new InsertOneOptions().bypassDocumentValidation(true));
                    collection.updateOne(session, TestHelper.getFilterFromId(objId1), new Document("$set", new Document("hello4", "test 4")));
                });

        applyOpsInTx(
                (collection, session) -> {
                    collection.updateOne(session, TestHelper.getFilterFromId(objId1), new Document("$set", new Document("hello2", "test 2")));
                    collection.updateOne(session, TestHelper.getFilterFromId(objId2), new Document("$set", new Document("hello3", "test 3")));
                });

        applyOpsInTx(
                (collection, session) -> collection.updateOne(session, TestHelper.getFilterFromId(objId1), new Document("$set", new Document("hello5", "test 5"))));

        applyOpsInTx(
                (collection, session) -> {
                    collection.deleteOne(session, TestHelper.getFilterFromId(objId1));
                    collection.deleteOne(session, TestHelper.getFilterFromId(objId2));
                });

        int[] sesseionCounts = { 3, 2, 1, 4 }; // deletes emit 2 events
        final SourceRecords records = consumeRecordsByTopic(Arrays.stream(sesseionCounts).sum());
        assertNoRecordsToConsume();

        Set<BsonTimestamp> timestamps = new HashSet<>();
        int rInd = 0;
        for (int sesseionCount : sesseionCounts) {
            BsonTimestamp ts = null;
            for (int i = 0, tInd = 0; i < sesseionCount; i++, tInd++, rInd++) {
                SourceRecord record = records.allRecordsInOrder().get(rInd);
                if (record.value() == null) {
                    tInd--;
                    continue; // these are second delete events that just contain empty value
                }
                RawBsonDocument oplog = new RawBsonDocument(((Struct) record.value()).getBytes(RAW_OPLOG_FIELD));
                Testing.print(oplog);
                assertThat(oplog.containsKey(TIMESTAMP));
                timestamps.add(oplog.getTimestamp(TIMESTAMP));
                if (ts == null) {
                    ts = oplog.getTimestamp(TIMESTAMP);
                }
                else {
                    assertThat(oplog.getTimestamp(TIMESTAMP)).isEqualTo(ts);
                }
                assertThat(oplog.containsKey(TXN_INDEX));
                assertThat(oplog.getInt32(TXN_INDEX).intValue()).isEqualTo(tInd + 1);
            }
        }
        assertThat(timestamps.size()).isEqualTo(sesseionCounts.length);
    }

    /**
     * Verifies that all transactional events should also have indices in source
     * Events in the same transaction should have the same timestamp. TXN_INDEX should be monotonically increasing
     * Here we set RAW_OPLOG_ENABLED to false as well
     * This also tests the recordParser
     *
     * @throws Exception if test fails
     */
    @Test
    public void transactionEventsSourceShouldHaveTXN_INDEX() throws Exception {
        // initialize this test with rawoplog disabled
        beforeEach(List.of(MongoDbConnectorConfig.RAW_OPLOG_ENABLED), List.of(false));

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

    private void applyOpsInTx(BiConsumer<MongoCollection<Document>, ClientSession> ops) {
        primary().execute("apply the operations in tx", mongo -> {
            Testing.debug("Apply ops in '" + MongoDbOplogTransactionIT.db + "." + MongoDbOplogTransactionIT.col + "'");
            // If the collection does not exist, be sure to create it
            final MongoDatabase db = mongo.getDatabase(MongoDbOplogTransactionIT.db);
            if (!collectionExists(db, MongoDbOplogTransactionIT.col)) {
                db.createCollection(MongoDbOplogTransactionIT.col);
            }

            final MongoCollection<Document> collection = db.getCollection(MongoDbOplogTransactionIT.col);

            try (ClientSession session = mongo.startSession()) {
                session.startTransaction();
                ops.accept(collection, session);
                session.commitTransaction();
            }
        });
    }
}

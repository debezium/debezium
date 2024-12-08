/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.config.CommonConnectorConfig.TASKS_MAX;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.data.Envelope;

public class MongoDbMultiTaskConnectorIT extends AbstractMongoConnectorIT {

    private static final String db = "mongdbmultitask";
    private static final String col = "collection1";

    @Before
    public void beforeEach() {
        initializeConnectorTestFramework();

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_with_pre_image")
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, db);
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    /**
     * Verifies that when multitask is enabled and there is only one task, the behavior should be identical to the regular scenario.
     * The task should consume all the events consumed by the connector.
     * @throws Exception
     */
    @Test
    public void multiTaskModeWithASingleTasksShouldGenerateRecordForAllEvents() throws Exception {
        final int numDocs = 100;
        final int numUpdates = 35;
        int hopSize = 100;
        config = config.edit()
                .with(TASKS_MAX, 1)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_GEN, 0)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_HOP_SECONDS, hopSize)
                .build();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo", 0);

        // Insert records
        List<ObjectId> docIds = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            ObjectId objId = new ObjectId();
            Document obj = new Document("_id", objId);

            insertDocuments(db, col, obj);
            docIds.add(objId);
        }

        // Consume the records of all the inserts and verify that they match the inserted documents
        final SourceRecords inserts = consumeRecordsByTopic(numDocs);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numDocs);
        for (int i = 0; i < numDocs; i++) {
            SourceRecord record = inserts.allRecordsInOrder().get(i);
            assertThat(TestHelper.getDocumentId(record)).isEqualTo(TestHelper.formatObjectId(docIds.get(i)));
            validateEventInWindow(record, hopSize, 1);
        }

        // Update some documents
        List<ObjectId> updatedDocIds = new ArrayList<>(numUpdates);
        for (int i = 0; i < numUpdates; i++) {
            ObjectId id = docIds.get((int) (Math.random() * docIds.size()));
            updatedDocIds.add(id);
            updateDocument(db, col, TestHelper.getFilterFromId(id), new Document("$set", new Document("update", i)));
        }

        // Consume the records of all the updates and verify that they match the updates
        final SourceRecords updates = consumeRecordsByTopic(numUpdates);
        assertNoRecordsToConsume();
        assertThat(updates.allRecordsInOrder().size()).isEqualTo(numUpdates);
        for (int i = 0; i < numUpdates; i++) {
            SourceRecord record = updates.allRecordsInOrder().get(i);
            assertThat(TestHelper.getDocumentId(record)).isEqualTo(TestHelper.formatObjectId(updatedDocIds.get(i)));
            assertThat(((Struct) record.value()).getStruct("updateDescription").getString("updatedFields"))
                    .isEqualTo("{\"update\": " + i + "}");
            validateEventInWindow(record, hopSize, 1);
        }

        // Delete the documents
        for (int i = 0; i < numDocs; i++) {
            deleteDocuments(db, col, TestHelper.getFilterFromId(docIds.get(i)));
        }

        // Consume the records of all the deletes and verify that they match the deleted documents
        final SourceRecords deletes = consumeRecordsByTopic(2 * numDocs);
        assertNoRecordsToConsume();
        assertThat(deletes.allRecordsInOrder().size()).isEqualTo(2 * numDocs);
        for (int i = 0; i < numDocs; i++) {
            SourceRecord record = deletes.allRecordsInOrder().get(2 * i);
            assertThat(TestHelper.getDocumentId(record)).isEqualTo(TestHelper.formatObjectId(docIds.get(i)));
            validateEventInWindow(record, hopSize, 1);
        }
    }

    /**
     * Verifies that each task only consumes the events that they are assigned, which defaults to using the timestamp as the property.
     * @throws Exception
     */
    @Test
    public void multiTaskModeWithMultipleTasksShouldGenerateRecordForOnlyAssignedEvents() throws Exception {
        final int numTasks = 4;
        final int numDocs = 100; // numDocs should be a multiple of numTasks to simply the test logic
        final int hopsize = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_GEN, 0)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_HOP_SECONDS, hopsize)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; task++) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Insert records
        Set<String> docIdStrs = new HashSet<>();
        List<ObjectId> docIds = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            ObjectId objId = new ObjectId();
            Document obj = new Document("_id", objId);

            insertDocuments(db, col, obj);
            docIdStrs.add(TestHelper.formatObjectId(objId));
            docIds.add(objId);

            if (i % 10 == 0) {
                Thread.sleep(1000);
            }
        }

        // Consume the records of all the inserts and verify that they match the inserted documents
        // and that they should be assigned to this task
        final SourceRecords inserts = consumeRecordsByTopic(numDocs);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numDocs);
        for (SourceRecord record : inserts.allRecordsInOrder()) {
            assertThat(TestHelper.getDocumentId(record)).isIn(docIdStrs);
            // validate time is in window of step
            validateEventInWindow(record, hopsize, numTasks);
        }

        // updates records
        Set<String> updateDocIdStrs = new HashSet<>();
        List<ObjectId> updatedDocIds = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            ObjectId id = docIds.get(i);
            updateDocument(db, col, TestHelper.getFilterFromId(id), new Document("$set", new Document("update", "success")));
            updateDocIdStrs.add(TestHelper.formatObjectId(id));
            updatedDocIds.add(id);
            if (i % 20 == 0) {
                Thread.sleep(1000);
            }
        }

        // Consume the records of all the udpates and verify that they match the inserted documents
        final SourceRecords updates = consumeRecordsByTopic(numDocs);
        assertNoRecordsToConsume();
        assertThat(updates.allRecordsInOrder().size()).isEqualTo(numDocs);
        for (SourceRecord record : updates.allRecordsInOrder()) {
            assertThat(TestHelper.getDocumentId(record)).isIn(updateDocIdStrs);
            validateEventInWindow(record, hopsize, numTasks);
        }

        // Delete the documents
        Collections.shuffle(docIds); // shuffle the list so that deletes are not in the same order as inserts
        for (int i = 0; i < numDocs; i++) {
            deleteDocuments(db, col, TestHelper.getFilterFromId(docIds.get(i)));
            if (i % 20 == 0) {
                Thread.sleep(1000);
            }
        }

        // Consume the records of all the deletes
        final SourceRecords deletes = consumeRecordsByTopic(numDocs * 2);
        assertNoRecordsToConsume();
        assertThat(deletes.allRecordsInOrder().size()).isEqualTo(numDocs * 2);
        for (SourceRecord record : deletes.allRecordsInOrder()) {
            assertThat(TestHelper.getDocumentId(record)).isIn(docIdStrs);
            // skip validating tombstone events as they will always be in the same window as the preceding delete event
            if (record.valueSchema() != null) {
                validateEventInWindow(record, hopsize, numTasks);
            }
        }
    }

    private long getClusterTime(SourceRecord record) {
        final Object recordValue = record.value();
        if (recordValue != null && recordValue instanceof Struct) {
            final Struct value = (Struct) recordValue;
            Struct t = value.getStruct(Envelope.FieldName.SOURCE);
            final long clusterTime = t.getInt64(Envelope.FieldName.TIMESTAMP);
            return clusterTime;
        }

        return -1;
    }

    private void validateEventInWindow(SourceRecord record, int hopSize, int taskCount) {
        final int taskId = Integer.parseInt((String) record.sourcePartition().get("task_id"));
        final long clusterTime = getClusterTime(record);

        BsonTimestamp ts = new BsonTimestamp((int) (clusterTime / 1000), 0);
        MultiTaskWindowHandler mth = new MultiTaskWindowHandler(ts, hopSize, taskCount, taskId);
        if (mth.windowStart.getTime() > ts.getTime() || mth.windowStop.getTime() < ts.getTime()) {
            throw new RuntimeException("Event timestamp " + ts.getTime() + " is not within the expected window: " + mth.windowStart + " - " + mth.windowStop);
        }
        assertThat(ts.getTime()).isGreaterThanOrEqualTo(mth.windowStart.getTime());
        assertThat(ts.getTime()).isLessThanOrEqualTo(mth.windowStop.getTime());
    }
}

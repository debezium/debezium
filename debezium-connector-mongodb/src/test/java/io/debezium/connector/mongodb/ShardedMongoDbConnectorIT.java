/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.model.Updates;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;

public class ShardedMongoDbConnectorIT extends AbstractShardedMongoConnectorIT {

    public static final String TOPIC_PREFIX = "mongo";
    private static final int INIT_DOCUMENT_COUNT = 1000;
    private static final int NEW_DOCUMENT_COUNT = 4;
    private static final int STOPPED_NEW_DOCUMENT_COUNT = 5;

    protected static void populateCollection(String dbName, String colName, int count) {
        populateCollection(dbName, colName, 0, count);
    }

    protected static void populateCollection(String dbName, String colName, int startId, int count) {
        try (var client = connect()) {
            var db = client.getDatabase(dbName);
            var collection = db.getCollection(colName);

            var items = IntStream.range(startId, startId + count)
                    .mapToObj(i -> new Document("_id", i).append("name", "name_" + i))
                    .collect(Collectors.toList());
            collection.insertMany(items);
        }
    }

    @Test
    public void shouldConsumeAllEventsFromDatabase() throws InterruptedException {
        var documentCount = 0;
        var topic = String.format("%s.%s.%s", TOPIC_PREFIX, shardedDatabase(), shardedCollection());

        // Populate collection
        populateCollection(shardedDatabase(), shardedCollection(), INIT_DOCUMENT_COUNT);
        documentCount += INIT_DOCUMENT_COUNT;

        // Use the DB configuration to define the connector's configuration ...
        Configuration config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TOPIC_PREFIX)
                .build();

        // Start the connector ...
        start(MongoDbConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        consumeAndVerifyFromInitialSync(topic, INIT_DOCUMENT_COUNT);

        // At this point, the connector has performed the initial sync and awaits changes ...

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is still running
        // ---------------------------------------------------------------------------------------------------------------
        populateCollection(shardedDatabase(), shardedCollection(), documentCount, NEW_DOCUMENT_COUNT);
        documentCount += NEW_DOCUMENT_COUNT;

        // Wait until we can consume the documents we just added ...
        consumeAndVerifyNotFromInitialSync(topic, NEW_DOCUMENT_COUNT);

        // ---------------------------------------------------------------------------------------------------------------
        // Stop the connector
        // ---------------------------------------------------------------------------------------------------------------
        stopConnector();

        // ---------------------------------------------------------------------------------------------------------------
        // Store more documents while the connector is NOT running
        // ---------------------------------------------------------------------------------------------------------------
        populateCollection(shardedDatabase(), shardedCollection(), documentCount, STOPPED_NEW_DOCUMENT_COUNT);
        documentCount += STOPPED_NEW_DOCUMENT_COUNT;

        // ---------------------------------------------------------------------------------------------------------------
        // Start the connector and we should only see the documents added since it was stopped
        // ---------------------------------------------------------------------------------------------------------------
        start(MongoDbConnector.class, config);
        consumeAndVerifyNotFromInitialSync(topic, STOPPED_NEW_DOCUMENT_COUNT);

        // ---------------------------------------------------------------------------------------------------------------
        // Update a document
        // ---------------------------------------------------------------------------------------------------------------
        try (var client = connect()) {
            var db = client.getDatabase(shardedDatabase());
            var collection = db.getCollection(shardedCollection());
            collection.updateOne(new Document("_id", 0), Updates.set("name", "Tom"));
        }
        consumeAndVerifyNotFromInitialSync(topic, 1, Envelope.Operation.UPDATE);
    }

    protected void consumeAndVerifyFromInitialSync(String topic, int expectedRecords) throws InterruptedException {
        var records = consumeRecordsByTopic(expectedRecords);
        assertThat(records.topics().size()).isEqualTo(1);
        assertThat(records.recordsForTopic(topic).size()).isEqualTo(expectedRecords);

        AtomicInteger lastCount = new AtomicInteger();
        var expectedLastCount = 1;
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyFromInitialSync(record, expectedLastCount, lastCount);
            verifyOperation(record, Envelope.Operation.READ);
        });
        assertThat(lastCount.get()).isEqualTo(expectedLastCount);
    }

    protected void verifyFromInitialSync(SourceRecord record, int numOfShards, AtomicInteger lastCounter) {
        if (record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)) {
            assertThat(record.sourceOffset().containsKey(SourceInfo.INITIAL_SYNC)).isTrue();
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("true");
        }
        else {
            // Only the last record in the initial sync should be marked as not being part of the initial sync ...
            assertThat(lastCounter.getAndIncrement()).isLessThanOrEqualTo(numOfShards);
            Struct value = (Struct) record.value();
            assertThat(value.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.SNAPSHOT_KEY)).isEqualTo("last");
        }
    }

    protected void consumeAndVerifyNotFromInitialSync(String topic, int expectedRecords) throws InterruptedException {
        consumeAndVerifyNotFromInitialSync(topic, expectedRecords, Envelope.Operation.CREATE);
    }

    protected void consumeAndVerifyNotFromInitialSync(String topic, int expectedRecords, Envelope.Operation op) throws InterruptedException {
        var records = consumeRecordsByTopic(expectedRecords);
        assertThat(records.recordsForTopic(topic).size()).isEqualTo(expectedRecords);
        assertThat(records.topics().size()).isEqualTo(1);
        records.forEach(record -> {
            // Check that all records are valid, and can be serialized and deserialized ...
            validate(record);
            verifyNotFromInitialSync(record);
            verifyOperation(record, op);
        });
    }
}

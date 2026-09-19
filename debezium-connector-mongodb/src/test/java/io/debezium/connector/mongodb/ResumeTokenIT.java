/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.JsonSerializationMode;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;

class ResumeTokenIT extends AbstractMongoConnectorIT {

    private static final String DATABASE = "tokens";
    private static final String COLLECTION = "records";
    private static final String TOPIC = "mongo1." + DATABASE + "." + COLLECTION;

    @ParameterizedTest
    @EnumSource(JsonSerializationMode.class)
    @FixFor("debezium/dbz#718")
    void shouldExposeCrudTokensAndResumeFromThePayload(JsonSerializationMode mode) throws InterruptedException {
        configure(mode, 0);
        try (var client = connect()) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
            collection.insertOne(new Document("_id", 0).append("value", "snapshot"));

            start(MongoDbConnector.class, config);
            final var snapshot = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
            final var snapshotValue = (Struct) snapshot.value();
            assertThat(snapshotValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.READ.code());
            assertThat(snapshotValue.getStruct(Envelope.FieldName.SOURCE).getString(SourceInfo.RESUME_TOKEN)).isNull();
            waitForStreamingRunning("mongodb", "mongo1");

            // Observe the same stream independently of the connector's SourceInfo and offset serialization.
            try (var observer = MongoUtils.openChangeStream(client, context).cursor()) {
                collection.insertOne(new Document("_id", 1).append("value", "created"));
                final var created = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
                final var createToken = assertResumeTokenMatchesEvent(created, readChangeStreamEvent(observer));

                collection.updateOne(new Document("_id", 1), new Document("$set", new Document("value", "updated")));
                final var updated = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
                assertThat(((Struct) updated.value()).getString(Envelope.FieldName.AFTER)).isNull();
                final var updateToken = assertResumeTokenMatchesEvent(updated, readChangeStreamEvent(observer));

                collection.replaceOne(new Document("_id", 1), new Document("_id", 1).append("value", "replaced"));
                final var replaced = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
                final var replaceToken = assertResumeTokenMatchesEvent(replaced, readChangeStreamEvent(observer));

                collection.deleteOne(new Document("_id", 1));
                final var deletes = consumeRecordsByTopic(2).recordsForTopic(TOPIC);
                assertThat(deletes).hasSize(2);
                final var deleted = deletes.get(0);
                final var tombstone = deletes.get(1);
                assertThat(((Struct) deleted.value()).getString(Envelope.FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
                final var deleteToken = assertResumeTokenMatchesEvent(deleted, readChangeStreamEvent(observer));
                assertThat(tombstone.value()).isNull();
                assertThat(List.of(createToken, updateToken, replaceToken, deleteToken)).doesNotHaveDuplicates();
                assertThat(updated.key()).isEqualTo(created.key());
                assertThat(replaced.key()).isEqualTo(created.key());
                assertThat(deleted.key()).isEqualTo(created.key());

                try (var replay = MongoUtils.openChangeStream(client, context).resumeAfter(createToken).cursor()) {
                    for (var expectedToken : List.of(updateToken, replaceToken, deleteToken)) {
                        assertThat(readChangeStreamEvent(replay).getResumeToken()).isEqualTo(expectedToken);
                    }
                }

                try (var resumed = MongoUtils.openChangeStream(client, context).resumeAfter(deleteToken).cursor()) {
                    final var marker = new Document("_id", 2).append("value", "after-delete");
                    collection.insertOne(marker);
                    assertThat(readChangeStreamEvent(resumed).getFullDocument()).isEqualTo(marker.toBsonDocument());
                    final var recordAfterDelete = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
                    assertResumeTokenMatchesEvent(recordAfterDelete, readChangeStreamEvent(observer));
                }

                stopConnector();
                start(MongoDbConnector.class, config);
                waitForStreamingRunning("mongodb", "mongo1");
                collection.insertOne(new Document("_id", 3).append("value", "after-restart"));
                final var recordAfterRestart = consumeRecordsByTopic(1).recordsForTopic(TOPIC).get(0);
                assertResumeTokenMatchesEvent(recordAfterRestart, readChangeStreamEvent(observer));
                assertNoRecordsToConsume();
            }
        }
    }

    @Test
    @FixFor("debezium/dbz#718")
    void shouldKeepDataTokensSeparateFromFilteredEventsAndHeartbeats() throws InterruptedException {
        configure(JsonSerializationMode.EXTENDED, 100);
        config = config.edit().with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.NO_DATA).build();
        context = new MongoDbTaskContext(config);
        try (var client = connect()) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
            client.getDatabase(DATABASE).createCollection(COLLECTION);
            start(MongoDbConnector.class, config);
            waitForStreamingRunning("mongodb", "mongo1");

            try (var observer = MongoUtils.openChangeStream(client, context).cursor()) {
                collection.insertOne(new Document("_id", 1));
                final var first = consumeUntilTopic(TOPIC);
                final var firstToken = assertResumeTokenMatchesEvent(first, readChangeStreamEvent(observer));
                final var firstSource = ((Struct) first.value()).getStruct(Envelope.FieldName.SOURCE);

                client.getDatabase(DATABASE).getCollection("excluded").insertOne(new Document("_id", 1));
                final var heartbeat = consumeUntilTopic("__debezium-heartbeat.mongo1");
                assertThat(heartbeat.valueSchema().field(Envelope.FieldName.SOURCE)).isNull();

                collection.insertOne(new Document("_id", 2));
                final var second = consumeUntilTopic(TOPIC);
                final var secondToken = assertResumeTokenMatchesEvent(second, readChangeStreamEvent(observer));
                assertThat(secondToken).isNotEqualTo(firstToken);
                assertThat(BsonDocument.parse(firstSource.getString(SourceInfo.RESUME_TOKEN))).isEqualTo(firstToken);
            }
        }
    }

    private void configure(JsonSerializationMode mode, int heartbeatIntervalMillis) {
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE + "." + COLLECTION)
                .with(MongoDbConnectorConfig.JSON_SERIALIZATION_MODE, mode)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, CaptureMode.CHANGE_STREAMS)
                .with(Heartbeat.HEARTBEAT_INTERVAL, heartbeatIntervalMillis)
                .build();
        context = new MongoDbTaskContext(config);
    }

    private SourceRecord consumeUntilTopic(String topic) throws InterruptedException {
        final var records = consumeRecordsByTopicUntil((count, record) -> topic.equals(record.topic())).recordsForTopic(topic);
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static BsonDocument assertResumeTokenMatchesEvent(SourceRecord record, ChangeStreamDocument<BsonDocument> event) {
        final var source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
        final var payloadToken = BsonDocument.parse(source.getString(SourceInfo.RESUME_TOKEN));
        final var offsetToken = ResumeTokens.fromBase64((String) record.sourceOffset().get(SourceInfo.RESUME_TOKEN));
        assertThat(payloadToken).isEqualTo(event.getResumeToken());
        assertThat(offsetToken).isEqualTo(payloadToken);
        return payloadToken;
    }

}

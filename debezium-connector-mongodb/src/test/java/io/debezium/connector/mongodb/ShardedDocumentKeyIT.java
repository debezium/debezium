/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * Verifies that a document snapshotted and then streamed produces the same change event key, which is what incremental
 * snapshot deduplication and log compaction rely on. Only a real sharded cluster can answer this, because the key of a
 * streamed event comes from MongoDB rather than from the connector.
 *
 * @author Soyeong Choe
 */
public class ShardedDocumentKeyIT extends AbstractShardedMongoConnectorIT {

    private static final String DATABASE = "dbit";
    private static final String COLLECTION = "orders";
    private static final String SHARD_KEY = "tenant";
    private static final String FULL_COLLECTION_NAME = DATABASE + "." + COLLECTION;
    private static final String TOPIC = "mongo1." + FULL_COLLECTION_NAME;

    @Override
    protected String shardedDatabase() {
        return DATABASE;
    }

    @Override
    protected Map<String, String> shardedCollections() {
        return Map.of(COLLECTION, SHARD_KEY);
    }

    @Test
    @FixFor("debezium/dbz#2337")
    void documentsWithSameIdOnDifferentShardsShouldHaveDistinctKeys() throws InterruptedException {
        insertTheSameIdOnBothShards();

        start(MongoDbConnector.class, config(MongoDbConnectorConfig.ChangeEventKeyMode.DOCUMENT_KEY));
        final var snapshotted = consumeRecordsByTopic(2).recordsForTopic(TOPIC);
        assertThat(snapshotted).hasSize(2).allSatisfy(record -> {
            verifyOperation(record, Envelope.Operation.READ);
            assertThat(keyFieldNameOf(record)).isEqualTo("documentKey");
        });
        assertThat(snapshotted).extracting(ShardedDocumentKeyIT::keyOf).containsExactlyInAnyOrder(
                "{\"tenant\": \"a\",\"_id\": 1}", "{\"tenant\": \"b\",\"_id\": 1}");

        try (var client = connect()) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
            assertThat(collection.updateOne(Filters.and(Filters.eq("_id", 1), Filters.eq(SHARD_KEY, "a")),
                    Updates.set("name", "Sally")).getModifiedCount()).isEqualTo(1);
            assertThat(collection.updateOne(Filters.and(Filters.eq("_id", 1), Filters.eq(SHARD_KEY, "b")),
                    Updates.set("name", "Peter")).getModifiedCount()).isEqualTo(1);
        }

        final var streamed = consumeRecordsByTopic(2).recordsForTopic(TOPIC);
        assertThat(streamed).hasSize(2).allSatisfy(record -> {
            verifyOperation(record, Envelope.Operation.UPDATE);
            assertThat(keyFieldNameOf(record)).isEqualTo("documentKey");
        });
        assertThat(streamed).extracting(ShardedDocumentKeyIT::keyOf)
                .containsExactlyInAnyOrderElementsOf(snapshotted.stream().map(ShardedDocumentKeyIT::keyOf).toList());
    }

    @Test
    @FixFor("debezium/dbz#2337")
    void defaultModeShouldGiveBothDocumentsTheSameKey() throws InterruptedException {
        insertTheSameIdOnBothShards();

        start(MongoDbConnector.class, config(MongoDbConnectorConfig.ChangeEventKeyMode.ID));
        final var snapshotted = consumeRecordsByTopic(2).recordsForTopic(TOPIC);

        // The collision the option exists to remove: two distinct documents under one key.
        assertThat(snapshotted).hasSize(2).allSatisfy(record -> assertThat(keyFieldNameOf(record)).isEqualTo("id"));
        assertThat(snapshotted).extracting(ShardedDocumentKeyIT::keyOf).containsExactly("1", "1");
    }

    /**
     * Places one chunk on each shard and inserts a document into each, both with an _id of 1. MongoDB only accepts the
     * second insert because the _id index is enforced per shard.
     */
    private void insertTheSameIdOnBothShards() {
        Assumptions.assumeTrue(mongo.size() >= 2);

        try (var client = connect()) {
            final var collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
            final var admin = client.getDatabase("admin");

            // Use range sharding so that the two tenants can be placed on different shards explicitly.
            collection.drop();
            admin.runCommand(new Document("shardCollection", FULL_COLLECTION_NAME)
                    .append("key", new Document(SHARD_KEY, 1)));
            admin.runCommand(new Document("split", FULL_COLLECTION_NAME)
                    .append("middle", new Document(SHARD_KEY, "b")));

            final var primaryShard = client.getDatabase("config").getCollection("databases")
                    .find(Filters.eq("_id", DATABASE)).first().getString("primary");
            final var destinationShard = mongo.getShard(0).getName().equals(primaryShard)
                    ? mongo.getShard(1).getName()
                    : mongo.getShard(0).getName();
            admin.runCommand(new Document("moveChunk", FULL_COLLECTION_NAME)
                    .append("find", new Document(SHARD_KEY, "b"))
                    .append("to", destinationShard));

            collection.insertOne(new Document("_id", 1).append(SHARD_KEY, "a").append("name", "Mary"));
            collection.insertOne(new Document("_id", 1).append(SHARD_KEY, "b").append("name", "John"));
        }
    }

    @Test
    @FixFor("debezium/dbz#2337")
    void snapshotAndStreamingShouldAgreeOnTheKey() throws InterruptedException {
        insertDocuments(DATABASE, COLLECTION, new Document("_id", 1)
                .append(SHARD_KEY, "a")
                .append("name", "Mary"));

        start(MongoDbConnector.class, config(MongoDbConnectorConfig.ChangeEventKeyMode.DOCUMENT_KEY));
        var snapshotted = consumeFirstRecord();

        updateName();
        var streamed = consumeFirstRecord();

        assertThat(keyFieldNameOf(streamed)).isEqualTo("documentKey");
        assertThat(keyOf(streamed)).isEqualTo(keyOf(snapshotted));
        assertThat(keyOf(snapshotted)).isEqualTo("{\"tenant\": \"a\",\"_id\": 1}");
    }

    @Test
    @FixFor("debezium/dbz#2337")
    void defaultModeShouldKeepTheIdKey() throws InterruptedException {
        insertDocuments(DATABASE, COLLECTION, new Document("_id", 1)
                .append(SHARD_KEY, "a")
                .append("name", "Mary"));

        start(MongoDbConnector.class, config(MongoDbConnectorConfig.ChangeEventKeyMode.ID));
        var snapshotted = consumeFirstRecord();

        updateName();
        var streamed = consumeFirstRecord();

        assertThat(keyFieldNameOf(streamed)).isEqualTo("id");
        assertThat(keyOf(streamed)).isEqualTo(keyOf(snapshotted));
        assertThat(keyOf(snapshotted)).isEqualTo("1");
    }

    private Configuration config(MongoDbConnectorConfig.ChangeEventKeyMode keyMode) {
        return TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, FULL_COLLECTION_NAME)
                .with(MongoDbConnectorConfig.CHANGE_EVENT_KEY_MODE, keyMode.getValue())
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .build();
    }

    private void updateName() {
        try (var client = connect()) {
            client.getDatabase(DATABASE)
                    .getCollection(COLLECTION)
                    .updateOne(Filters.eq("_id", 1), Updates.set("name", "Sally"));
        }
    }

    private SourceRecord consumeFirstRecord() throws InterruptedException {
        var records = consumeRecordsByTopic(1).recordsForTopic(TOPIC);
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static String keyFieldNameOf(SourceRecord record) {
        assertThat(record.keySchema().fields()).hasSize(1);
        return record.keySchema().fields().get(0).name();
    }

    private static String keyOf(SourceRecord record) {
        var key = (Struct) record.key();
        return key.getString(keyFieldNameOf(record));
    }
}

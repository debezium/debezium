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
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import io.debezium.config.Configuration;
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Verifies that {@code document_key} mode works on a deployment that has no shards, where the connector resolves no
 * shard key and reads nothing from {@code config.collections}.
 *
 * @author Soyeong Choe
 */
public class ReplicaSetDocumentKeyIT extends AbstractMongoConnectorIT {

    private static final String DATABASE = "dbit";
    private static final String COLLECTION = "orders";
    private static final String TOPIC = "mongo1." + DATABASE + "." + COLLECTION;

    @Test
    @FixFor("debezium/dbz#2337")
    void documentKeyModeShouldKeyByIdAlone() throws InterruptedException {
        insertDocuments(DATABASE, COLLECTION, new Document("_id", 1).append("name", "Mary"));

        start(MongoDbConnector.class, TestHelper.getConfiguration(mongo)
                .edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, DATABASE)
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, DATABASE + "." + COLLECTION)
                .with(MongoDbConnectorConfig.CHANGE_EVENT_KEY_MODE,
                        MongoDbConnectorConfig.ChangeEventKeyMode.DOCUMENT_KEY.getValue())
                .with(MongoDbConnectorConfig.SNAPSHOT_MODE, MongoDbConnectorConfig.SnapshotMode.INITIAL)
                .build());
        final var snapshotted = consumeFirstRecord();

        updateDocument(DATABASE, COLLECTION, new Document("_id", 1), new Document("$set", new Document("name", "Sally")));
        final var streamed = consumeFirstRecord();

        // The mode still names the field, but without a shard key the document key holds only _id.
        assertThat(keyFieldNameOf(snapshotted)).isEqualTo("documentKey");
        assertThat(keyOf(streamed)).isEqualTo(keyOf(snapshotted));
        assertThat(keyOf(snapshotted)).isEqualTo("{\"_id\": 1}");
    }

    private SourceRecord consumeFirstRecord() throws InterruptedException {
        final var records = consumeRecordsByTopic(1).recordsForTopic(TOPIC);
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private static String keyFieldNameOf(SourceRecord record) {
        assertThat(record.keySchema().fields()).hasSize(1);
        return record.keySchema().fields().get(0).name();
    }

    private static String keyOf(SourceRecord record) {
        return ((Struct) record.key()).getString(keyFieldNameOf(record));
    }
}

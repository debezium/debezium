/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

public class JsonSerializationTest {

    private JsonSerialization serialization = new JsonSerialization(MongoDbConnectorConfig.JsonSerializationMode.LEGACY);

    @Test
    void shouldGeOnlyIdFromCompositeKey() {
        var id = new BsonInt32(42);
        var composite = new BsonDocument("email", new BsonString("foo@bar.com"))
                .append("_id", id);

        var key = serialization.getDocumentId(composite);

        Assertions.assertThat(key).isEqualTo("42");
    }

    @Test
    void shouldGetEqualDocumentIdFromSimpleAndComposite() {
        var id = new BsonObjectId();
        var simple = new BsonDocument("_id", id);
        var composite = new BsonDocument("email", new BsonString("foo@bar.com"))
                .append("_id", id);

        var simpleKey = serialization.getDocumentId(simple);
        var compositeKey = serialization.getDocumentId(composite);

        Assertions.assertThat(compositeKey).isEqualTo(simpleKey);
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldKeepWholeDocumentKeyOfShardedCollection() {
        var documentKey = new BsonDocument("caseNo", new BsonString("201907130000200001"))
                .append("_id", new BsonObjectId(new ObjectId("5d2974673484856dfa2b909a")));

        var key = serialization.getDocumentKey(documentKey);

        // The key uses the compact writer settings, so fields are separated by ',' without a trailing space
        Assertions.assertThat(key).isEqualTo("{\"caseNo\": \"201907130000200001\",\"_id\": {\"$oid\": \"5d2974673484856dfa2b909a\"}}");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldKeepWholeDocumentKeyOfUnshardedCollection() {
        var documentKey = new BsonDocument("_id", new BsonObjectId(new ObjectId("5d2974673484856dfa2b909a")));

        var key = serialization.getDocumentKey(documentKey);

        Assertions.assertThat(key).isEqualTo("{\"_id\": {\"$oid\": \"5d2974673484856dfa2b909a\"}}");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldDistinguishSameIdOnDifferentShards() {
        var id = new BsonString("duplicate-id");
        var onShardA = new BsonDocument("tenant", new BsonString("a")).append("_id", id);
        var onShardB = new BsonDocument("tenant", new BsonString("b")).append("_id", id);

        Assertions.assertThat(serialization.getDocumentId(onShardA)).isEqualTo(serialization.getDocumentId(onShardB));
        Assertions.assertThat(serialization.getDocumentKey(onShardA)).isNotEqualTo(serialization.getDocumentKey(onShardB));
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldReturnNullDocumentKeyForNullInput() {
        Assertions.assertThat(serialization.getDocumentKey(null)).isNull();
    }

    @Test
    void shouldPreserveLegacyUpdatedFieldsFormatting() {
        var updatedFields = new BsonDocument("name", new BsonString("Mary"))
                .append("zipcode", new BsonString("11111"));

        Assertions.assertThat(serialization.getUpdatedFields(updatedFields)).isEqualTo("{\"name\": \"Mary\", \"zipcode\": \"11111\"}");
    }

}

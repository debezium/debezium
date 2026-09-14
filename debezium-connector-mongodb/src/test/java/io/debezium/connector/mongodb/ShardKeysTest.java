/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;

/**
 * Verifies that the document key derived from a full document during a snapshot matches the {@code documentKey} that
 * MongoDB reports on a change stream event for the same document. The two have to be identical, otherwise incremental
 * snapshot deduplication and log compaction break.
 */
public class ShardKeysTest {

    private static final BsonObjectId ID = new BsonObjectId();
    private static final CollectionId COLLECTION = new CollectionId("dbA", "c1");

    @Test
    @FixFor("DBZ-2337")
    void shouldUseOnlyIdWhenCollectionIsNotSharded() {
        var document = new BsonDocument("_id", ID)
                .append("name", new BsonString("Mary"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of());

        assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldPutShardKeyBeforeId() {
        var document = new BsonDocument("_id", ID)
                .append("caseNo", new BsonString("201907130000200001"))
                .append("name", new BsonString("Mary"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("caseNo"));

        // Field order matters, the serialized key has to match the change stream documentKey byte for byte
        assertThat(documentKey).isEqualTo(
                new BsonDocument("caseNo", new BsonString("201907130000200001")).append("_id", ID));
        assertThat(List.copyOf(documentKey.keySet())).containsExactly("caseNo", "_id");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldPreserveCompoundShardKeyOrder() {
        var document = new BsonDocument("_id", ID)
                .append("region", new BsonString("eu"))
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("tenant", "region"));

        assertThat(List.copyOf(documentKey.keySet())).containsExactly("tenant", "region", "_id");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldNotDuplicateIdWhenItIsPartOfTheShardKey() {
        var document = new BsonDocument("_id", ID)
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("tenant", "_id"));

        assertThat(List.copyOf(documentKey.keySet())).containsExactly("tenant", "_id");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldKeepIdInShardKeyPosition() {
        var document = new BsonDocument("_id", ID)
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("_id", "tenant"));

        assertThat(List.copyOf(documentKey.keySet())).containsExactly("_id", "tenant");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldResolveNestedShardKeyUnderItsDottedName() {
        var document = new BsonDocument("_id", ID)
                .append("address", new BsonDocument("zip", new BsonString("12345")));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        // MongoDB reports a nested shard key under its dotted name, not as a nested document
        assertThat(documentKey).isEqualTo(
                new BsonDocument("address.zip", new BsonString("12345")).append("_id", ID));
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldSkipShardKeyFieldMissingFromDocument() {
        var document = new BsonDocument("_id", ID);

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldSkipNestedShardKeyWhoseParentIsNotADocument() {
        var document = new BsonDocument("_id", ID)
                .append("address", new BsonString("not a document"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldReadShardKeyOrderFromConfigEntry() {
        var entry = new Document("key", new Document("tenant", 1).append("region", "hashed"));

        assertThat(ShardKeys.shardKeyPathsOf(entry, COLLECTION)).containsExactly("tenant", "region");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldTreatMissingConfigEntryAsUnsharded() {
        assertThat(ShardKeys.shardKeyPathsOf(null, COLLECTION)).isEmpty();
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldFailWhenConfigEntryHasNoShardKey() {
        // Falling back to _id here would not match the documentKey that the change stream reports for the same document
        assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(new Document(), COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldFailWhenConfigEntryHasEmptyShardKey() {
        var entry = new Document("key", new Document());

        assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(entry, COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldFailWhenConfigEntryShardKeyIsNotADocument() {
        var entry = new Document("key", "tenant");

        assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(entry, COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    @FixFor("DBZ-2337")
    void shouldTreatEveryCollectionAsUnshardedWithoutConnection() {
        assertThat(ShardKeys.unsharded().shardKeyPathsFor(COLLECTION)).isEmpty();
    }
}

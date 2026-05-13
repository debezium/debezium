/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;

public class MongoDbIncrementalSnapshotContextTest {

    /**
     * The MongoDB incremental snapshot context must preserve additional conditions across
     * serialization round-trips (i.e., across connector restarts). Previously
     * {@code stringToDataCollections} only restored the collection ID, silently dropping the
     * {@code additional_condition} field, which caused the snapshot to resume with no filter
     * on every chunk after a restart.
     */
    @Test
    @FixFor("debezium/dbz#1807")
    public void shouldPreserveAdditionalConditionAcrossOffsetRoundTrip() {
        final String collectionId = "dbA.c1";
        final String filter = "{aa: {$lt: 250}}";

        final MongoDbIncrementalSnapshotContext<CollectionId> original = new MongoDbIncrementalSnapshotContext<>(false);

        final AdditionalCondition condition = AdditionalCondition.AdditionalConditionBuilder.builder()
                .dataCollection(Pattern.compile(collectionId, Pattern.CASE_INSENSITIVE))
                .filter(filter)
                .build();

        original.addDataCollectionNamesToSnapshot("test-correlation", List.of(collectionId), List.of(condition), "");
        // Set state required for store() to actually serialize the data collections
        original.sendEvent(new Object[]{ "k" });
        original.maximumKey(new Object[]{ "max" });

        final Map<String, Object> offsets = new HashMap<>();
        original.store(offsets);

        final MongoDbIncrementalSnapshotContext<CollectionId> restored = MongoDbIncrementalSnapshotContext.load(offsets, false);

        final DataCollection<CollectionId> restoredCollection = restored.currentDataCollectionId();
        assertThat(restoredCollection).isNotNull();
        assertThat(restoredCollection.getId().identifier()).isEqualTo(collectionId);
        assertThat(restoredCollection.getAdditionalCondition()).hasValue("(" + filter + ")");
    }

    /**
     * Verifies that repeated serialization round-trips do not accumulate parentheses on the
     * additional condition. Without the parenthesis-stripping logic in
     * {@code stringToDataCollections}, the field would re-wrap on each cycle, eventually
     * breaking {@code Document.parse} when the condition is applied to a MongoDB query.
     */
    @Test
    @FixFor("debezium/dbz#1807")
    public void shouldNotAccumulateParenthesesAcrossMultipleRoundTrips() {
        final String collectionId = "dbA.c1";
        final String filter = "{aa: {$lt: 250}}";

        MongoDbIncrementalSnapshotContext<CollectionId> context = new MongoDbIncrementalSnapshotContext<>(false);

        final AdditionalCondition condition = AdditionalCondition.AdditionalConditionBuilder.builder()
                .dataCollection(Pattern.compile(collectionId, Pattern.CASE_INSENSITIVE))
                .filter(filter)
                .build();

        context.addDataCollectionNamesToSnapshot("test-correlation", List.of(collectionId), List.of(condition), "");
        context.sendEvent(new Object[]{ "k" });
        context.maximumKey(new Object[]{ "max" });

        for (int i = 0; i < 3; i++) {
            final Map<String, Object> offsets = new HashMap<>();
            context.store(offsets);
            context = MongoDbIncrementalSnapshotContext.load(offsets, false);
            context.sendEvent(new Object[]{ "k" });
            context.maximumKey(new Object[]{ "max" });
        }

        final DataCollection<CollectionId> restoredCollection = context.currentDataCollectionId();
        assertThat(restoredCollection.getAdditionalCondition()).hasValue("(" + filter + ")");
    }

    /**
     * Verifies that a collection with no additional condition round-trips cleanly.
     */
    @Test
    @FixFor("debezium/dbz#1807")
    public void shouldHandleEmptyAdditionalConditionAcrossOffsetRoundTrip() {
        final String collectionId = "dbA.c1";

        final MongoDbIncrementalSnapshotContext<CollectionId> original = new MongoDbIncrementalSnapshotContext<>(false);

        original.addDataCollectionNamesToSnapshot("test-correlation", List.of(collectionId), List.of(), "");
        original.sendEvent(new Object[]{ "k" });
        original.maximumKey(new Object[]{ "max" });

        final Map<String, Object> offsets = new HashMap<>();
        original.store(offsets);

        final MongoDbIncrementalSnapshotContext<CollectionId> restored = MongoDbIncrementalSnapshotContext.load(offsets, false);

        final DataCollection<CollectionId> restoredCollection = restored.currentDataCollectionId();
        assertThat(restoredCollection).isNotNull();
        assertThat(restoredCollection.getId().identifier()).isEqualTo(collectionId);
        assertThat(restoredCollection.getAdditionalCondition()).isEmpty();
    }
}

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
import java.util.OptionalLong;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;

public class MongoDbIncrementalSnapshotContextTest {

    @Test
    @FixFor("debezium/dbz#2717")
    public void shouldResumeFromBeginningOfFirstWindow() {
        final var context = snapshotContext();
        context.startNewChunk();
        context.nextChunkPosition(new Object[]{ 10 });
        context.sendEvent(new Object[]{ 9 });

        final var restored = roundTrip(context);
        assertThat(restored.snapshotRunning()).isTrue();
        assertThat(restored.chunkEndPosititon()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2717")
    public void shouldResumeUnorderedWindowFromItsStart() {
        final var context = snapshotContext();
        context.nextChunkPosition(new Object[]{ 50 });
        context.startNewChunk();
        context.nextChunkPosition(new Object[]{ 60 });
        context.sendEvent(new Object[]{ 59 });
        assertThat(roundTrip(context).chunkEndPosititon()).containsExactly(50);

        context.sendEvent(new Object[]{ 53 });
        final var restored = roundTrip(context);
        assertThat(restored.chunkEndPosititon()).containsExactly(50);
        // A second checkpoint before any new record must retain the restored boundary.
        assertThat(roundTrip(restored).chunkEndPosititon()).containsExactly(50);

        context.startNewChunk();
        context.nextChunkPosition(new Object[]{ 70 });
        context.sendEvent(new Object[]{ 69 });
        assertThat(roundTrip(context).chunkEndPosititon()).containsExactly(60);
    }

    @Test
    @FixFor("debezium/dbz#2717")
    public void shouldRevertToStartOfUnfinishedWindow() {
        final var context = snapshotContext();
        context.nextChunkPosition(new Object[]{ 50 });
        context.startNewChunk();
        context.nextChunkPosition(new Object[]{ 60 });
        context.sendEvent(new Object[]{ 59 });

        context.revertChunk();
        assertThat(context.chunkEndPosititon()).containsExactly(50);
        context.startNewChunk();
        assertThat(roundTrip(context).chunkEndPosititon()).containsExactly(50);
    }

    @Test
    @FixFor("debezium/dbz#2717")
    public void shouldClearWindowBoundaryForNextCollection() {
        final var context = snapshotContext();
        context.nextChunkPosition(new Object[]{ 50 });
        context.startNewChunk();
        context.nextChunkPosition(new Object[]{ 60 });
        context.sendEvent(new Object[]{ 59 });

        context.nextDataCollection();
        final var restored = roundTrip(context);
        assertThat(restored.currentDataCollectionId().getId().identifier()).isEqualTo("dbA.c2");
        assertThat(restored.chunkEndPosititon()).isNull();

        context.nextDataCollection();
        assertThat(context.store(new HashMap<>())).isEmpty();
        assertThat(roundTrip(context).snapshotRunning()).isFalse();
    }

    private MongoDbIncrementalSnapshotContext<CollectionId> snapshotContext() {
        final var context = new MongoDbIncrementalSnapshotContext<CollectionId>(false);
        context.addDataCollectionNamesToSnapshot("test-correlation", List.of("dbA.c1", "dbA.c2"), List.of(), "");
        context.maximumKey(new Object[]{ 1_000 });
        return context;
    }

    private MongoDbIncrementalSnapshotContext<CollectionId> roundTrip(MongoDbIncrementalSnapshotContext<CollectionId> context) {
        return MongoDbIncrementalSnapshotContext.load(context.store(new HashMap<>()), false);
    }

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

    /**
     * The best-effort per-collection total document count must survive an offset round-trip so that
     * progress reporting continues to work after a connector restart.
     */
    @Test
    @FixFor("debezium/dbz#2620")
    public void shouldRoundTripTotalRowsThroughOffsets() {
        final MongoDbIncrementalSnapshotContext<CollectionId> original = new MongoDbIncrementalSnapshotContext<>(false);
        original.addDataCollectionNamesToSnapshot("test-correlation", List.of("dbA.c1"), List.of(), "");
        original.sendEvent(new Object[]{ "k" });
        original.maximumKey(new Object[]{ "max" });
        original.totalRows(OptionalLong.of(4200L));

        final Map<String, Object> offsets = new HashMap<>();
        original.store(offsets);

        final MongoDbIncrementalSnapshotContext<CollectionId> restored = MongoDbIncrementalSnapshotContext.load(offsets, false);
        assertThat(restored.totalRows()).isEqualTo(OptionalLong.of(4200L));
    }

    /**
     * When the total document count could not be established it must not be stored, and a restored
     * context must report it as absent so that the progress fields are omitted.
     */
    @Test
    @FixFor("debezium/dbz#2620")
    public void shouldNotStoreTotalRowsWhenAbsent() {
        final MongoDbIncrementalSnapshotContext<CollectionId> original = new MongoDbIncrementalSnapshotContext<>(false);
        original.addDataCollectionNamesToSnapshot("test-correlation", List.of("dbA.c1"), List.of(), "");
        original.sendEvent(new Object[]{ "k" });
        original.maximumKey(new Object[]{ "max" });

        final Map<String, Object> offsets = new HashMap<>();
        original.store(offsets);

        assertThat(offsets).doesNotContainKey(MongoDbIncrementalSnapshotContext.TABLE_TOTAL_ROWS);
        final MongoDbIncrementalSnapshotContext<CollectionId> restored = MongoDbIncrementalSnapshotContext.load(offsets, false);
        assertThat(restored.totalRows()).isEqualTo(OptionalLong.empty());
    }
}

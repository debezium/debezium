/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class SignalBasedIncrementalSnapshotContextTest {

    @Test
    public void shouldTagDataCollectionsWithTheRequestingSignalId() {
        final SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();

        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a", "public.b"), List.of(), "");
        context.addDataCollectionNamesToSnapshot("signal-2", List.of("public.c"), List.of(), "");

        assertThat(context.getDataCollections())
                .extracting(DataCollection::getCorrelationId)
                .containsExactly(Optional.of("signal-1"), Optional.of("signal-1"), Optional.of("signal-2"));
        assertThat(context.getCorrelationId()).isEqualTo("signal-2");
    }

    @Test
    public void shouldRoundTripPerCollectionCorrelationIdThroughOffsets() {
        final SignalBasedIncrementalSnapshotContext<TableId> context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a"), List.of(), "");
        context.addDataCollectionNamesToSnapshot("signal-2", List.of("public.b"), List.of(), "");

        final Map<String, Object> offsets = context.store(new HashMap<>());
        final IncrementalSnapshotContext<TableId> restored = SignalBasedIncrementalSnapshotContext.load(offsets);

        assertThat(restored.getDataCollections())
                .extracting(DataCollection::getCorrelationId)
                .containsExactly(Optional.of("signal-1"), Optional.of("signal-2"));
        assertThat(restored.getDataCollections())
                .extracting(dataCollection -> dataCollection.getId().identifier())
                .containsExactly("public.a", "public.b");
        assertThat(restored.getCorrelationId()).isEqualTo("signal-2");
    }

    @Test
    public void shouldLoadOffsetsWrittenWithPerCollectionCorrelationId() {
        final Map<String, Object> offsets = Map.of(
                "incremental_snapshot_collections",
                "[{\"incremental_snapshot_collections_id\":\"public.a\","
                        + "\"incremental_snapshot_collections_additional_condition\":null,"
                        + "\"incremental_snapshot_collections_surrogate_key\":null,"
                        + "\"incremental_snapshot_collections_correlation_id\":\"signal-1\"},"
                        + "{\"incremental_snapshot_collections_id\":\"public.b\","
                        + "\"incremental_snapshot_collections_additional_condition\":null,"
                        + "\"incremental_snapshot_collections_surrogate_key\":null,"
                        + "\"incremental_snapshot_collections_correlation_id\":\"signal-2\"}]");

        final IncrementalSnapshotContext<TableId> restored = SignalBasedIncrementalSnapshotContext.load(offsets);

        assertThat(restored.getDataCollections())
                .extracting(DataCollection::getCorrelationId)
                .containsExactly(Optional.of("signal-1"), Optional.of("signal-2"));
        assertThat(restored.getDataCollections())
                .extracting(dataCollection -> dataCollection.getId().identifier())
                .containsExactly("public.a", "public.b");
    }

    @Test
    public void shouldLoadOffsetsWrittenWithoutPerCollectionCorrelationId() {
        final Map<String, Object> offsets = Map.of(
                "incremental_snapshot_collections",
                "[{\"incremental_snapshot_collections_id\":\"public.a\","
                        + "\"incremental_snapshot_collections_additional_condition\":null,"
                        + "\"incremental_snapshot_collections_surrogate_key\":null}]");

        final IncrementalSnapshotContext<TableId> restored = SignalBasedIncrementalSnapshotContext.load(offsets);

        assertThat(restored.getDataCollections())
                .extracting(DataCollection::getCorrelationId)
                .containsExactly(Optional.empty());
        assertThat(restored.getDataCollections())
                .extracting(dataCollection -> dataCollection.getId().identifier())
                .containsExactly("public.a");
    }
}

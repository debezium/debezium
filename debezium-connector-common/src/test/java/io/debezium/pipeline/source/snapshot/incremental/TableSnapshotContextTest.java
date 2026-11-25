/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class TableSnapshotContextTest {

    private final SignalBasedIncrementalSnapshotContext<TableId> snapshotContext = new SignalBasedIncrementalSnapshotContext<>();
    private final TableSnapshotContext<TableId> context = new TableSnapshotContext<>(
            new DataCollection<>(new TableId(null, "s1", "a")), snapshotContext);

    @Test
    public void shouldStartAsInitialChunkWithoutRestartPoint() {
        assertThat(context.isNonInitialChunk()).isFalse();
        assertThat(context.lastEventKeySent()).isNull();
        assertThat(context.chunkEndPosititon()).isNull();
    }

    @Test
    public void shouldRememberTheKeyOfTheLastDispatchedRow() {
        context.nextChunkPosition(new Object[]{ 10 });

        context.sendEvent(new Object[]{ 3 });
        context.sendEvent(new Object[]{ 7 });

        assertThat(context.isNonInitialChunk()).isTrue();
        assertThat(context.lastEventKeySent()).containsExactly(7);
        assertThat(context.chunkEndPosititon()).containsExactly(10);
    }

    @Test
    public void shouldRevertToTheLastDispatchedRowNotToTheChunkEnd() {
        context.nextChunkPosition(new Object[]{ 10 });
        context.sendEvent(new Object[]{ 7 });

        context.revertChunk();

        assertThat(context.chunkEndPosititon()).containsExactly(7);
        assertThat(context.isNonInitialChunk()).isTrue();
    }

    @Test
    public void shouldRevertToTheInitialChunkWhenNothingWasDispatched() {
        context.nextChunkPosition(new Object[]{ 10 });

        context.revertChunk();

        assertThat(context.chunkEndPosititon()).isNull();
        assertThat(context.isNonInitialChunk()).isFalse();
    }

    @Test
    public void shouldDelegateSignalIdAndCollectionsToTheSnapshotContext() {
        snapshotContext.addDataCollectionNamesToSnapshot("signal-1", List.of("s1.a", "s1.b"), List.of(), "");

        assertThat(context.getCorrelationId()).isEqualTo("signal-1");
        assertThat(context.getDataCollections()).hasSize(2);
        assertThat(context.dataCollectionsToBeSnapshottedCount()).isEqualTo(2);
    }
}

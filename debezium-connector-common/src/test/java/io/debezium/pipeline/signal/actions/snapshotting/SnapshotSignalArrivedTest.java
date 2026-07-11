/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * Verifies the behaviour of {@code arrived()} for the snapshot signals, in particular that a malformed
 * {@code data-collections} is skipped rather than acted upon. A malformed stop signal must not escalate to
 * stopping the entire in-progress snapshot, while a missing/empty one still does.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/798">DBZ-6352</a>
 */
class SnapshotSignalArrivedTest {

    private final DocumentReader reader = DocumentReader.defaultReader();

    @SuppressWarnings("unchecked")
    private final EventDispatcher<Partition, ?> dispatcher = mock(EventDispatcher.class);
    @SuppressWarnings("unchecked")
    private final IncrementalSnapshotChangeEventSource<Partition, ?> incrementalSource = mock(IncrementalSnapshotChangeEventSource.class);

    @BeforeEach
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void setUp() {
        when(dispatcher.getIncrementalSnapshotChangeEventSource()).thenReturn((IncrementalSnapshotChangeEventSource) incrementalSource);
    }

    private SignalPayload<Partition> payload(String json) throws IOException {
        final Document data = reader.read(json);
        return new SignalPayload<>(mock(Partition.class), "signal-id", "stop-snapshot", data, mock(OffsetContext.class), Map.of());
    }

    @Test
    void stopSnapshotShouldNotStopAnythingForMalformedDataCollections() throws IOException, InterruptedException {
        final StopSnapshot<Partition> action = new StopSnapshot<>(dispatcher);

        final boolean result = action.arrived(payload("{\"data-collections\": [[\"schema.table1\"]]}"));

        assertThat(result).isFalse();
        // The whole point of the fix: a malformed stop signal must NOT escalate to stopping the snapshot.
        verify(incrementalSource, never()).requestStopSnapshot(any(), any(), any(), any());
    }

    @Test
    void stopSnapshotShouldStopAllForMissingDataCollections() throws IOException, InterruptedException {
        final StopSnapshot<Partition> action = new StopSnapshot<>(dispatcher);

        final boolean result = action.arrived(payload("{}"));

        assertThat(result).isTrue();
        // Missing data-collections is a valid request to stop the entire snapshot: null flows through to stop-all.
        verify(incrementalSource).requestStopSnapshot(any(), any(), any(), isNull());
    }

    @Test
    void stopSnapshotShouldStopSpecificCollections() throws IOException, InterruptedException {
        final StopSnapshot<Partition> action = new StopSnapshot<>(dispatcher);

        final boolean result = action.arrived(payload("{\"data-collections\": [\"schema.table1\"]}"));

        assertThat(result).isTrue();
        verify(incrementalSource).requestStopSnapshot(any(), any(), any(), eq(java.util.List.of("schema.table1")));
    }
}

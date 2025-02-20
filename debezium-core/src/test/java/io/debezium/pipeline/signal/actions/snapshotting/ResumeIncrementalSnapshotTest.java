/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.actions.snapshotting;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

public class ResumeIncrementalSnapshotTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testResumeSnapshotSignal() throws InterruptedException {
        EventDispatcher<Partition, DataCollectionId> dispatcher = mock(EventDispatcher.class);
        IncrementalSnapshotChangeEventSource<Partition, DataCollectionId> snapshotSource = mock(IncrementalSnapshotChangeEventSource.class);
        when(dispatcher.getIncrementalSnapshotChangeEventSource()).thenReturn(snapshotSource);

        ResumeIncrementalSnapshot<Partition> resumeIncrementalSnapshot = new ResumeIncrementalSnapshot<>(dispatcher);

        Partition partition = mock(Partition.class);
        OffsetContext offsetContext = mock(OffsetContext.class);
        SignalPayload<Partition> signalPayload = new SignalPayload<>(partition, null, null, null, offsetContext, null);

        boolean result = resumeIncrementalSnapshot.arrived(signalPayload);

        verify(snapshotSource).resumeSnapshot(partition, offsetContext);
        assertTrue(result);
    }
}

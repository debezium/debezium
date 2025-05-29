/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;

public class ChangeEventSourceCoordinatorTest {

    SnapshotterService snapshotterService;
    Snapshotter snapshotter;
    CommonConnectorConfig connectorConfig;
    ChangeEventSourceCoordinator coordinator;
    ChangeEventSource.ChangeEventSourceContext context;

    @Before
    public void before() {
        snapshotterService = mock(SnapshotterService.class);
        snapshotter = mock(Snapshotter.class);
        connectorConfig = mock(CommonConnectorConfig.class);
        when(connectorConfig.getLogicalName()).thenReturn("DummyConnector");
        coordinator = new ChangeEventSourceCoordinator(null, null, SourceConnector.class, connectorConfig, null,
                null, null, null, null, null, snapshotterService, null, null);
        context = mock(ChangeEventSource.ChangeEventSourceContext.class);
    }

    @Test
    public void testNotDelayStreamingIfSnapshotShouldNotStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(false);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, never()).getStreamingDelay();
    }

    @Test
    public void testDelayStreamingIfSnapshotShouldStream() throws Exception {
        when(snapshotterService.getSnapshotter()).thenReturn(snapshotter);
        when(snapshotter.shouldStream()).thenReturn(true);
        when(connectorConfig.getStreamingDelay()).thenReturn(Duration.of(1, ChronoUnit.SECONDS));
        when(context.isRunning()).thenReturn(true);

        coordinator.delayStreamingIfNeeded(context);

        verify(connectorConfig, times(1)).getStreamingDelay();
    }

}

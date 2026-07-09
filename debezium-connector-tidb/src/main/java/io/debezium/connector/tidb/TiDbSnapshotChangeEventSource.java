/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * Snapshot source of the TiDB connector.
 * <p>
 * Initial data snapshots are not implemented yet: a TiCDC changefeed created with a
 * {@code start-ts} in the past can backfill history, and TiDB's MySQL-compatible SQL endpoint
 * will be used for managed initial/incremental snapshots in a follow-up iteration. Until then
 * every snapshot is skipped and the connector goes straight to streaming.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotChangeEventSource implements SnapshotChangeEventSource<TiDbPartition, TiDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbSnapshotChangeEventSource.class);

    private final TiDbConnectorConfig connectorConfig;

    public TiDbSnapshotChangeEventSource(TiDbConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public SnapshotResult<TiDbOffsetContext> execute(ChangeEventSourceContext context, TiDbPartition partition,
                                                     TiDbOffsetContext previousOffset, SnapshottingTask snapshottingTask) {
        LOGGER.info("Snapshots are not supported by the TiDB connector yet, proceeding to streaming");
        final TiDbOffsetContext offset = previousOffset != null ? previousOffset : TiDbOffsetContext.empty(connectorConfig);
        return SnapshotResult.skipped(offset);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset) {
        return new SnapshottingTask(false, false, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, false, List.of(), Map.of(), true);
    }
}

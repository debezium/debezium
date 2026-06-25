/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Performs an initial snapshot of the ${connectorName} data source.
 *
 * <p>Override {@link #execute} to read existing data and emit READ events via the
 * {@link EventDispatcher}. When the snapshot is complete, update the offset context
 * so that streaming resumes at the correct position.
 */
class ${connectorName}SnapshotChangeEventSource
        implements SnapshotChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(${connectorName}SnapshotChangeEventSource.class);

    private final ${connectorName}ConnectorConfig config;
    private final TableId dataCollectionId;
    private final EventDispatcher<${connectorName}Partition, TableId> dispatcher;
    private final SnapshotProgressListener<${connectorName}Partition> progressListener;
    private final Clock clock;

    ${connectorName}SnapshotChangeEventSource(${connectorName}ConnectorConfig config,
                                               TableId dataCollectionId,
                                               EventDispatcher<${connectorName}Partition, TableId> dispatcher,
                                               SnapshotProgressListener<${connectorName}Partition> progressListener,
                                               Clock clock) {
        this.config = config;
        this.dataCollectionId = dataCollectionId;
        this.dispatcher = dispatcher;
        this.progressListener = progressListener;
        this.clock = clock;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(${connectorName}Partition partition,
                                                ${connectorName}OffsetContext offsetContext) {
        boolean offsetExists = offsetContext.getPosition() > 0;
        boolean shouldSnapshot = config.getSnapshotMode().shouldSnapshotData(offsetExists, false);
        LOGGER.info("Snapshot decision: mode={}, offsetExists={}, shouldSnapshot={}",
                config.getSnapshotMode(), offsetExists, shouldSnapshot);
        return new SnapshottingTask(shouldSnapshot, false, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(${connectorName}Partition partition,
                                                        ${connectorName}OffsetContext offsetContext,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return getSnapshottingTask(partition, offsetContext);
    }

    @Override
    public SnapshotResult<${connectorName}OffsetContext> execute(
            ChangeEventSource.ChangeEventSourceContext context,
            ${connectorName}Partition partition,
            ${connectorName}OffsetContext offsetContext,
            SnapshottingTask task) throws InterruptedException {

        if (task.shouldSkipSnapshot()) {
            LOGGER.info("Skipping snapshot – resuming streaming from position {}", offsetContext.getPosition());
            return SnapshotResult.skipped(offsetContext);
        }

        LOGGER.info("Starting ${connectorName} snapshot");

        // TODO: implement snapshot logic here.
        // For each existing record, read the raw row from your source, then dispatch:
        //   dispatcher.dispatchDataChangeEvent(partition, dataCollectionId,
        //       new ${connectorName}ChangeRecordEmitter(
        //           partition, offsetContext, Envelope.Operation.READ, rawRowData, clock, config));
        // Update offsetContext.setPosition(...) as you advance through the source.

        LOGGER.info("${connectorName} snapshot complete");
        return SnapshotResult.completed(offsetContext);
    }
}

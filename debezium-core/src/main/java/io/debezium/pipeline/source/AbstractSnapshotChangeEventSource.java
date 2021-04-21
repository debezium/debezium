/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.common.TaskPartition;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 * An abstract implementation of {@link SnapshotChangeEventSource} that all implementations should extend
 * to inherit common functionality.
 *
 * @author Chris Cranford
 */
public abstract class AbstractSnapshotChangeEventSource<P extends TaskPartition, O extends OffsetContext> implements SnapshotChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotChangeEventSource.class);

    private final CommonConnectorConfig connectorConfig;
    private final SnapshotProgressListener snapshotProgressListener;

    public AbstractSnapshotChangeEventSource(CommonConnectorConfig connectorConfig, SnapshotProgressListener snapshotProgressListener) {
        this.connectorConfig = connectorConfig;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context, P partition, O previousOffset) throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
        if (snapshottingTask.shouldSkipSnapshot()) {
            LOGGER.debug("Skipping snapshotting");
            return SnapshotResult.skipped(previousOffset);
        }

        delaySnapshotIfNeeded(context);

        final SnapshotContext ctx;
        try {
            ctx = prepare(context, partition);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        boolean completedSuccessfully = true;

        try {
            snapshotProgressListener.snapshotStarted();
            SnapshotResult result = doExecute(context, partition, previousOffset, ctx, snapshottingTask);

            return result;
        }
        catch (InterruptedException e) {
            completedSuccessfully = false;
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        }
        catch (Exception t) {
            completedSuccessfully = false;
            throw new DebeziumException(t);
        }
        finally {
            LOGGER.info("Snapshot - Final stage");
            complete(ctx);

            if (completedSuccessfully) {
                snapshotProgressListener.snapshotCompleted();
            }
            else {
                snapshotProgressListener.snapshotAborted();
            }
        }
    }

    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(final Collection<T> allDataCollections) {
        final Set<String> snapshotAllowedDataCollections = connectorConfig.getDataCollectionsToBeSnapshotted();
        if (snapshotAllowedDataCollections.size() == 0) {
            return allDataCollections.stream();
        }
        else {
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream().anyMatch(s -> dataCollectionId.identifier().matches(s)));
        }
    }

    /**
     * Delays snapshot execution as per the {@link CommonConnectorConfig#SNAPSHOT_DELAY_MS} parameter.
     */
    protected void delaySnapshotIfNeeded(ChangeEventSourceContext context) throws InterruptedException {
        Duration snapshotDelay = connectorConfig.getSnapshotDelay();

        if (snapshotDelay.isZero() || snapshotDelay.isNegative()) {
            return;
        }

        Threads.Timer timer = Threads.timer(Clock.SYSTEM, snapshotDelay);
        Metronome metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);

        while (!timer.expired()) {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while awaiting initial snapshot delay");
            }

            LOGGER.info("The connector will wait for {}s before proceeding", timer.remaining().getSeconds());
            metronome.pause();
        }
    }

    /**
     * Executes this source.  Implementations should regularly check via the given context if they should stop.  If
     * that's the case, they should abort their processing and perform any clean-up needed, such as rolling back
     * pending transactions, releasing locks, etc.
     *
     * @param context contextual information for this source's execution
     * @param partition
     * @param previousOffset
     * @param snapshotContext mutable context information populated throughout the snapshot process
     * @param snapshottingTask immutable information about what tasks should be performed during snapshot
     * @return an indicator to the position at which the snapshot was taken
     */
    protected abstract SnapshotResult doExecute(ChangeEventSourceContext context, P partition, O previousOffset,
                                                SnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception;

    /**
     * Returns the snapshotting task based on the previous offset (if available) and the connector's snapshotting mode.
     */
    protected abstract SnapshottingTask getSnapshottingTask(O previousOffset);

    /**
     * Prepares the taking of a snapshot and returns an initial {@link SnapshotContext}.
     */
    protected abstract SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext, P partition) throws Exception;

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot may have run successfully or have been aborted at this point.
     *
     * @param snapshotContext snapshot context
     */
    protected void complete(SnapshotContext snapshotContext) {
    }

    /**
     * Mutable context which is populated in the course of snapshotting
     */
    public static class SnapshotContext implements AutoCloseable {
        public OffsetContext offset;

        @Override
        public void close() throws Exception {
        }
    }

    /**
     * A configuration describing the task to be performed during snapshotting.
     */
    public static class SnapshottingTask {

        private final boolean snapshotSchema;
        private final boolean snapshotData;

        public SnapshottingTask(boolean snapshotSchema, boolean snapshotData) {
            this.snapshotSchema = snapshotSchema;
            this.snapshotData = snapshotData;
        }

        /**
         * Whether data (rows in captured tables) should be snapshotted.
         */
        public boolean snapshotData() {
            return snapshotData;
        }

        /**
         * Whether the schema of captured tables should be snapshotted.
         */
        public boolean snapshotSchema() {
            return snapshotSchema;
        }

        /**
         * Whether to skip the snapshot phase.
         *
         * By default this method will skip performing a snapshot if both {@link #snapshotSchema()} and
         * {@link #snapshotData()} return {@code false}.
         */
        public boolean shouldSkipSnapshot() {
            return !snapshotSchema() && !snapshotData();
        }

        @Override
        public String toString() {
            return "SnapshottingTask [snapshotSchema=" + snapshotSchema + ", snapshotData=" + snapshotData + "]";
        }
    }
}

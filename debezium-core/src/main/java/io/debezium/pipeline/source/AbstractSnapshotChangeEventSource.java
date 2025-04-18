/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * An abstract implementation of {@link SnapshotChangeEventSource} that all implementations should extend
 * to inherit common functionality.
 *
 * @author Chris Cranford
 */
public abstract class AbstractSnapshotChangeEventSource<P extends Partition, O extends OffsetContext> implements SnapshotChangeEventSource<P, O>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotChangeEventSource.class);

    /**
     * Interval for showing a log statement with the progress while scanning a single table.
     */
    public static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final CommonConnectorConfig connectorConfig;
    private final SnapshotProgressListener<P> snapshotProgressListener;

    protected final NotificationService<P, O> notificationService;

    public AbstractSnapshotChangeEventSource(CommonConnectorConfig connectorConfig, SnapshotProgressListener<P> snapshotProgressListener,
                                             NotificationService<P, O> notificationService) {
        this.connectorConfig = connectorConfig;
        this.snapshotProgressListener = snapshotProgressListener;
        this.notificationService = notificationService;
    }

    protected Offsets<P, OffsetContext> getOffsets(SnapshotContext<P, O> ctx, O previousOffset, SnapshottingTask snapshottingTask) {
        return Offsets.of(ctx.partition, previousOffset);
    }

    @Override
    public SnapshotResult<O> execute(ChangeEventSourceContext context, P partition, O previousOffset, SnapshottingTask snapshottingTask) throws InterruptedException {

        final SnapshotContext<P, O> ctx;
        try {
            ctx = prepare(partition, snapshottingTask.isOnDemand());
            connectorConfig.getBeanRegistry().add(StandardBeanNames.SNAPSHOT_CONTEXT, ctx);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        Offsets<P, OffsetContext> offsets = getOffsets(ctx, previousOffset, snapshottingTask);
        if (snapshottingTask.shouldSkipSnapshot()) {
            LOGGER.debug("Skipping snapshotting");
            notificationService.initialSnapshotNotificationService().notifySkipped(offsets.getTheOnlyPartition(), offsets.getTheOnlyOffset());
            return SnapshotResult.skipped(previousOffset);
        }

        delaySnapshotIfNeeded(context);

        boolean completedSuccessfully = true;

        try {
            snapshotProgressListener.snapshotStarted(partition);
            notificationService.initialSnapshotNotificationService().notifyStarted(offsets.getTheOnlyPartition(), offsets.getTheOnlyOffset());
            return doExecute(context, previousOffset, ctx, snapshottingTask);
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
            close();

            if (completedSuccessfully) {
                LOGGER.info("Snapshot completed");
                completed(ctx);
                snapshotProgressListener.snapshotCompleted(partition);

                notificationService.initialSnapshotNotificationService().notifyCompleted(ctx.partition, ctx.offset);
            }
            else {

                String basicWarnMessage = "Snapshot was not completed successfully";
                String finalWarnMessage = String.format("%s %s", basicWarnMessage, ", it will be re-executed upon connector restart");

                if (snapshottingTask.isOnDemand()) {
                    // In case of error blocking snapshot will not be automatically executed.
                    previousOffset.postSnapshotCompletion();
                    finalWarnMessage = basicWarnMessage;
                }

                LOGGER.warn(finalWarnMessage);
                aborted(ctx);
                snapshotProgressListener.snapshotAborted(offsets.getTheOnlyPartition());
                notificationService.initialSnapshotNotificationService().notifyAborted(ctx.partition, ctx.offset);
            }
        }
    }

    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(final Collection<T> allDataCollections,
                                                                                             Set<Pattern> snapshotAllowedDataCollections) {
        if (snapshotAllowedDataCollections.isEmpty()) {
            return allDataCollections.stream();
        }
        else {
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                            .anyMatch(tableNameMatcher(dataCollectionId)));
        }
    }

    private static <T extends DataCollectionId> Predicate<Pattern> tableNameMatcher(T dataCollectionId) {
        return s -> s.matcher(dataCollectionId.identifier()).matches() ||
                s.matcher(((TableId) dataCollectionId).toDoubleQuotedString()).matches();
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
     * @param previousOffset previous offset restored from Kafka
     * @param snapshotContext mutable context information populated throughout the snapshot process
     * @param snapshottingTask immutable information about what tasks should be performed during snapshot
     * @return an indicator to the position at which the snapshot was taken
     */
    protected abstract SnapshotResult<O> doExecute(ChangeEventSourceContext context, O previousOffset,
                                                   SnapshotContext<P, O> snapshotContext,
                                                   SnapshottingTask snapshottingTask)
            throws Exception;

    /**
     * Prepares the taking of a snapshot and returns an initial {@link SnapshotContext}.
     */
    protected abstract SnapshotContext<P, O> prepare(P partition, boolean onDemand) throws Exception;

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot may have run successfully or have been aborted at this point.
     *
     */
    @Override
    public void close() {
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot have run successfully at this point.
     *
     * @param snapshotContext snapshot context
     */
    protected void completed(SnapshotContext<P, O> snapshotContext) {
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot is aborted at this point.
     *
     * @param snapshotContext snapshot context
     */
    protected void aborted(SnapshotContext<P, O> snapshotContext) throws InterruptedException {
    }

    protected Set<Pattern> getDataCollectionPattern(List<String> dataCollections) {
        return dataCollections.stream()
                .map(tables -> Strings.setOfRegex(tables, Pattern.CASE_INSENSITIVE))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Mutable context which is populated in the course of snapshotting
     */
    public static class SnapshotContext<P extends Partition, O extends OffsetContext> implements AutoCloseable {
        public P partition;
        public O offset;

        public SnapshotContext(P partition) {
            this.partition = partition;
        }

        @Override
        public void close() throws Exception {
        }
    }

}

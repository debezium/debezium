/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.connector.mysql.GtidSet.GTID_DELIMITER;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.mysql.signal.KafkaSignal;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

@NotThreadSafe
public class MySqlReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlReadOnlyIncrementalSnapshotContext.class);
    private GtidSet previousLowWatermark;
    private GtidSet previousHighWatermark;
    private GtidSet lowWatermark;
    private GtidSet highWatermark;
    private Long signalOffset;
    private final Queue<KafkaSignal> kafkaSignals = new ConcurrentLinkedQueue<>();
    public static final String SIGNAL_OFFSET = INCREMENTAL_SNAPSHOT_KEY + "_signal_offset";

    public MySqlReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MySqlReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    protected static <U> IncrementalSnapshotContext<U> init(MySqlReadOnlyIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        AbstractIncrementalSnapshotContext.init(context, offsets);
        final Long signalOffset = (Long) offsets.get(SIGNAL_OFFSET);
        context.setSignalOffset(signalOffset);
        return context;
    }

    public static <U> MySqlReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> MySqlReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        MySqlReadOnlyIncrementalSnapshotContext<U> context = new MySqlReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public void setLowWatermark(GtidSet lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public void setHighWatermark(GtidSet highWatermark) {
        this.highWatermark = highWatermark.subtract(lowWatermark);
    }

    public boolean updateWindowState(OffsetContext offsetContext) {
        String currentGtid = getCurrentGtid(offsetContext);
        if (!windowOpened && lowWatermark != null) {
            boolean pastLowWatermark = !lowWatermark.contains(currentGtid);
            if (pastLowWatermark) {
                LOGGER.debug("Current gtid {}, low watermark {}", currentGtid, lowWatermark);
                windowOpened = true;
            }
        }
        if (windowOpened && highWatermark != null) {
            boolean pastHighWatermark = !highWatermark.contains(currentGtid);
            if (pastHighWatermark) {
                LOGGER.debug("Current gtid {}, high watermark {}", currentGtid, highWatermark);
                closeWindow();
                return true;
            }
        }
        return false;
    }

    public boolean reachedHighWatermark(String currentGtid) {
        if (highWatermark == null) {
            return false;
        }
        if (currentGtid == null) {
            return true;
        }
        String[] gtid = GTID_DELIMITER.split(currentGtid);
        GtidSet.UUIDSet uuidSet = getUuidSet(gtid[0]);
        if (uuidSet != null) {
            long maxTransactionId = uuidSet.getIntervals().stream()
                    .mapToLong(GtidSet.Interval::getEnd)
                    .max()
                    .getAsLong();
            if (maxTransactionId <= Long.parseLong(gtid[1])) {
                LOGGER.debug("Gtid {} reached high watermark {}", currentGtid, highWatermark);
                return true;
            }
        }
        return false;
    }

    public String getCurrentGtid(OffsetContext offsetContext) {
        return offsetContext.getSourceInfo().getString(SourceInfo.GTID_KEY);
    }

    public void closeWindow() {
        windowOpened = false;
        previousHighWatermark = highWatermark;
        highWatermark = null;
        previousLowWatermark = lowWatermark;
        lowWatermark = null;
    }

    private GtidSet.UUIDSet getUuidSet(String serverId) {
        return highWatermark.getUUIDSets().isEmpty() ? lowWatermark.forServerWithId(serverId) : highWatermark.forServerWithId(serverId);
    }

    public boolean serverUuidChanged() {
        return highWatermark.getUUIDSets().size() > 1;
    }

    public Long getSignalOffset() {
        return signalOffset;
    }

    public void setSignalOffset(Long signalOffset) {
        this.signalOffset = signalOffset;
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        Map<String, Object> snapshotOffset = super.store(offset);
        snapshotOffset.put(SIGNAL_OFFSET, signalOffset);
        return snapshotOffset;
    }

    public void enqueueKafkaSignal(KafkaSignal signal) {
        kafkaSignals.add(signal);
    }

    public KafkaSignal getKafkaSignals() {
        return kafkaSignals.poll();
    }

    public boolean hasKafkaSignals() {
        return !kafkaSignals.isEmpty();
    }

    public boolean watermarksChanged() {
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }
}

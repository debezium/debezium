/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.SourceInfo;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbGtidSet.MariaDbGtid;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbGtidSet.MariaDbStreamSet;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

public class MariaDbReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbReadOnlyIncrementalSnapshotContext.class);
    private MariaDbGtidSet previousLowWatermark;
    private MariaDbGtidSet previousHighWatermark;
    private MariaDbGtidSet lowWatermark;
    private MariaDbGtidSet highWatermark;
    private Long signalOffset;
    public static final String SIGNAL_OFFSET = INCREMENTAL_SNAPSHOT_KEY + "_signal_offset";

    public MariaDbReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MariaDbReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    protected static <U> IncrementalSnapshotContext<U> init(MariaDbReadOnlyIncrementalSnapshotContext<U> context, Map<String, ?> offsets) {
        AbstractIncrementalSnapshotContext.init(context, offsets);
        final Long signalOffset = (Long) offsets.get(SIGNAL_OFFSET);
        context.setSignalOffset(signalOffset);
        return context;
    }

    public static <U> MariaDbReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> MariaDbReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        MariaDbReadOnlyIncrementalSnapshotContext<U> context = new MariaDbReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public void setLowWatermark(MariaDbGtidSet lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public void setHighWatermark(MariaDbGtidSet highWatermark) {
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

        final MariaDbGtid currentMariaDbGtid = MariaDbGtidSet.parse(currentGtid);
        final MariaDbStreamSet streamSet = getStreamSetForGtid(currentMariaDbGtid);
        if (streamSet != null) {
            long maxSequenceId = streamSet.stream()
                    .mapToLong(MariaDbGtid::getSequence)
                    .max()
                    .getAsLong();
            if (maxSequenceId <= currentMariaDbGtid.getSequence()) {
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

    private MariaDbStreamSet getStreamSetForGtid(MariaDbGtid currentGtid) {
        return highWatermark.isKnown(currentGtid)
                ? highWatermark.forGtidStream(currentGtid)
                : lowWatermark.forGtidStream(currentGtid);
    }

    public boolean serverStreamSetChanged() {
        return !highWatermark.isEmpty();
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

    public boolean watermarksChanged() {
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }
}

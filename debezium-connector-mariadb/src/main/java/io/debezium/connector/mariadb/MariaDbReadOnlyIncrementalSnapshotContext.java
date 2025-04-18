/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet.MariaDbGtid;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet.MariaDbStreamSet;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * @author Chris Cranford
 */
public class MariaDbReadOnlyIncrementalSnapshotContext<T> extends BinlogReadOnlyIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbReadOnlyIncrementalSnapshotContext.class);

    private MariaDbGtidSet previousLowWatermark;
    private MariaDbGtidSet previousHighWatermark;
    private MariaDbGtidSet lowWatermark;
    private MariaDbGtidSet highWatermark;

    public MariaDbReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MariaDbReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    @Override
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

    @Override
    public boolean hasServerIdentifierChanged() {
        return serverStreamSetChanged();
    }

    @Override
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

    @Override
    public boolean watermarksChanged() {
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }

    @Override
    public void closeWindow() {
        windowOpened = false;
        previousHighWatermark = highWatermark;
        highWatermark = null;
        previousLowWatermark = lowWatermark;
        lowWatermark = null;
    }

    @Override
    public void setLowWatermark(GtidSet lowWatermark) {
        this.lowWatermark = (MariaDbGtidSet) lowWatermark;
    }

    @Override
    public void setHighWatermark(GtidSet highWatermark) {
        this.highWatermark = (MariaDbGtidSet) highWatermark.subtract(lowWatermark);
    }

    private MariaDbStreamSet getStreamSetForGtid(MariaDbGtid currentGtid) {
        return highWatermark.isEmpty()
                ? lowWatermark.forGtidStream(currentGtid)
                : highWatermark.forGtidStream(currentGtid);
    }

    public boolean serverStreamSetChanged() {
        return !highWatermark.isEmpty();
    }

    public static <U> MariaDbReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> MariaDbReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        MariaDbReadOnlyIncrementalSnapshotContext<U> context = new MariaDbReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }
}

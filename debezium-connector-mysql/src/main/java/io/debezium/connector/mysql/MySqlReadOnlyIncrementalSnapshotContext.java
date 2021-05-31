/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.connector.mysql.GtidSet.GTID_DELIMITER;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

@NotThreadSafe
public class MySqlReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlReadOnlyIncrementalSnapshotContext.class);
    private GtidSet lowWatermark = null;
    private GtidSet highWatermark = null;

    public MySqlReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MySqlReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
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
        String currentGtid = offsetContext.getSourceInfo().getString(SourceInfo.GTID_KEY);
        if (!windowOpened && lowWatermark != null) {
            boolean pastLowWatermark = !lowWatermark.contains(currentGtid);
            if (pastLowWatermark) {
                LOGGER.debug("Current gtid {}, low watermark {}", currentGtid, lowWatermark);
                windowOpened = true;
                lowWatermark = null;
            }
        }
        if (windowOpened && highWatermark != null) {
            boolean pastHighWatermark = !highWatermark.contains(currentGtid);
            if (pastHighWatermark) {
                LOGGER.debug("Current gtid {}, high watermark {}", currentGtid, highWatermark);
                windowOpened = false;
                highWatermark = null;
                return true;
            }
        }
        return false;
    }

    public boolean reachedHighWatermark(OffsetContext offsetContext) {
        String currentGtid = offsetContext.getSourceInfo().getString(SourceInfo.GTID_KEY);
        if (highWatermark == null) {
            return false;
        }
        if (currentGtid == null) {
            return true;
        }
        String[] gtid = GTID_DELIMITER.split(currentGtid);
        GtidSet.UUIDSet uuidSet = highWatermark.forServerWithId(gtid[0]);
        if (uuidSet != null) {
            long maxTransactionId = uuidSet.getIntervals().stream()
                    .mapToLong(GtidSet.Interval::getEnd)
                    .max()
                    .getAsLong();
            if (maxTransactionId <= Long.parseLong(gtid[1])) {
                LOGGER.debug("Heartbeat {} reached high watermark {}", currentGtid, highWatermark);
                windowOpened = false;
                highWatermark = null;
                lowWatermark = null;
                return true;
            }
        }
        return false;
    }

    public boolean serverUuidChanged() {
        return highWatermark.getUUIDSets().size() > 1;
    }
}

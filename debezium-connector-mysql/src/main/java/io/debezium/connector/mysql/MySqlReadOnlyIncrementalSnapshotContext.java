/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.MySqlGtid;

import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.pipeline.spi.OffsetContext;

public class MySqlReadOnlyIncrementalSnapshotContext<T> extends BinlogReadOnlyIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlReadOnlyIncrementalSnapshotContext.class);

    private MySqlGtidSet previousLowWatermark;
    private MySqlGtidSet previousHighWatermark;
    private MySqlGtidSet lowWatermark;
    private MySqlGtidSet highWatermark;

    public MySqlReadOnlyIncrementalSnapshotContext() {
        this(true);
    }

    public MySqlReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    @Override
    public void setLowWatermark(GtidSet lowWatermark) {
        this.lowWatermark = (MySqlGtidSet) lowWatermark;
    }

    @Override
    public void setHighWatermark(GtidSet highWatermark) {
        this.highWatermark = (MySqlGtidSet) highWatermark.subtract(lowWatermark);
    }

    @Override
    public boolean hasServerIdentifierChanged() {
        return serverUuidChanged();
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
    public boolean reachedHighWatermark(String currentGtid) {
        if (highWatermark == null) {
            return false;
        }
        if (currentGtid == null) {
            return true;
        }
        final MySqlGtid gtid = MySqlGtid.fromString(currentGtid);
        MySqlGtidSet.UUIDSet uuidSet = getUuidSet(gtid.getServerId().toString());
        if (uuidSet != null) {
            long maxTransactionId = uuidSet.getIntervals().stream()
                    .mapToLong(MySqlGtidSet.Interval::getEnd)
                    .max()
                    .getAsLong();
            if (maxTransactionId <= gtid.getTransactionId()) {
                LOGGER.debug("Gtid {} reached high watermark {}", currentGtid, highWatermark);
                return true;
            }
        }
        return false;
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
    public boolean watermarksChanged() {
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }

    private MySqlGtidSet.UUIDSet getUuidSet(String serverId) {
        return highWatermark.getUUIDSets().isEmpty() ? lowWatermark.forServerWithId(serverId) : highWatermark.forServerWithId(serverId);
    }

    private boolean serverUuidChanged() {
        return highWatermark.getUUIDSets().size() > 1;
    }

    public static <U> MySqlReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> MySqlReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        MySqlReadOnlyIncrementalSnapshotContext<U> context = new MySqlReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }
}

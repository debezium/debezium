/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.Map;

import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * Abstract common base class for binlog-based connector read only incremental snapshot context state.
 *
 * @author Chris Cranford
 */
public abstract class BinlogReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    public static final String SIGNAL_OFFSET = INCREMENTAL_SNAPSHOT_KEY + "_signal_offset";

    private Long signalOffset;

    public BinlogReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
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

    public String getCurrentGtid(OffsetContext offsetContext) {
        return offsetContext.getSourceInfo().getString(BinlogSourceInfo.GTID_KEY);
    }

    /**
     * Set the GTID as the low watermark.
     *
     * @param gtidSet the global transaction identifier set; should not be null
     */
    public abstract void setLowWatermark(GtidSet gtidSet);

    /**
     * Set the GTID as the high watermark.
     *
     * @param gtidSet the global transaction identifier set; should not be null
     */
    public abstract void setHighWatermark(GtidSet gtidSet);

    /**
     * Checks whether the GTID has reached the high watermark.
     *
     * @param currentGtid the GTID to be checked
     * @return true if the high watermark has been reached; false otherwise
     */
    public abstract boolean reachedHighWatermark(String currentGtid);

    /**
     * @return whether the global transaction identifier's server id has changed
     */
    public abstract boolean hasServerIdentifierChanged();

    /**
     * Update the window state in the offsets.
     *
     * @param offsetContext the offsets to be updated; should not be null
     * @return true if the process should end; false if it should continue
     */
    public abstract boolean updateWindowState(OffsetContext offsetContext);

    /**
     * @return true if the watermarks have changed; false otherwise
     */
    public abstract boolean watermarksChanged();

    /**
     * Closes the current open incremental snapshot window.
     */
    public abstract void closeWindow();

    /**
     * Initialize the incremental snapshot context.
     *
     * @param context the read only incremental snapshot context, should not be null
     * @param offsets the connector offsets, should not be null
     * @return the initialized context, never null
     */
    protected static <U> IncrementalSnapshotContext<U> init(BinlogReadOnlyIncrementalSnapshotContext<U> context,
                                                            Map<String, ?> offsets) {
        AbstractIncrementalSnapshotContext.init(context, offsets);
        final Long signalOffset = (Long) offsets.get(SIGNAL_OFFSET);
        context.setSignalOffset(signalOffset);
        return context;
    }
}

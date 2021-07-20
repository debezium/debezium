/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

@NotThreadSafe
public class ReadOnlyMySqlIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadOnlyMySqlIncrementalSnapshotContext.class);
    private GtidSet lowWatermark = null;
    private GtidSet highWatermark = null;

    public ReadOnlyMySqlIncrementalSnapshotContext() {
        this(true);
    }

    public ReadOnlyMySqlIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> ReadOnlyMySqlIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, true);
    }

    public static <U> ReadOnlyMySqlIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        ReadOnlyMySqlIncrementalSnapshotContext<U> context = new ReadOnlyMySqlIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public void setLowWatermark(GtidSet lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public void setHighWatermark(GtidSet highWatermark) {
        this.highWatermark = highWatermark;
    }

    public boolean updateWindowState(OffsetContext offsetContext) {
        String currentGtid = offsetContext.getSourceInfo().getString(SourceInfo.GTID_KEY);
        if (windowOpened) {
            if (highWatermark != null) {
                boolean pastHighWatermark = !highWatermark.contains(currentGtid);
                if (pastHighWatermark) {
                    LOGGER.debug("Current gtid {}, high watermark {}", currentGtid, highWatermark);
                    windowOpened = false;
                    highWatermark = null;
                    return true;
                }
            }
        }
        if (lowWatermark != null) {
            boolean pastLowWatermark = !lowWatermark.contains(currentGtid);
            if (pastLowWatermark) {
                LOGGER.debug("Current gtid {}, low watermark {}", currentGtid, lowWatermark);
                windowOpened = true;
                lowWatermark = null;
            }
        }
        return false;
    }
}

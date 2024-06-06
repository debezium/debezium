/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

public class PostgresReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotContext.class);

    private PgSnapshot lowWatermark;
    private PgSnapshot highWatermark;
    private PgSnapshot previousHighWatermark;
    private PgSnapshot previousLowWatermark;

    public PostgresReadOnlyIncrementalSnapshotContext() {
        this(false);
    }

    public PostgresReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, false);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        PostgresReadOnlyIncrementalSnapshotContext<U> context = new PostgresReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public PgSnapshot getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(PgSnapshot lowWatermark) {
        LOGGER.trace("Setting low watermark to {}", lowWatermark);
        this.lowWatermark = lowWatermark;
    }

    public PgSnapshot getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(PgSnapshot highWatermark) {
        LOGGER.trace("Setting high watermark to {}", highWatermark);
        this.highWatermark = highWatermark;
    }

    public void updateWindowState(OffsetContext offsetContext) {

        Long eventTxId = offsetContext.getSourceInfo().getInt64(SourceInfo.TXID_KEY);
        LOGGER.trace("Received event with TxId {}", eventTxId);
        LOGGER.trace("Updating window. Window oped: {}, low watermark {}, high watermark {}", windowOpened, lowWatermark, highWatermark);

        if (!windowOpened && lowWatermark != null) {
            boolean pastLowWatermark = eventTxId >= lowWatermark.getXMin();
            if (pastLowWatermark) {
                LOGGER.debug("Current event txId {}, low watermark {}", eventTxId, lowWatermark);
                windowOpened = true;
            }
        }
        if (windowOpened && highWatermark != null) {
            boolean pastHighWatermark = eventTxId > Math.max(highWatermark.getXMax(), lowWatermark.getXMax());
            if (pastHighWatermark) {
                LOGGER.debug("Current event txId {}, high watermark {}", eventTxId, highWatermark);
                closeWindow();
            }
        }
    }

    public boolean isWindowClosed() {
        return !windowOpened;
    }

    public void closeWindow() {
        LOGGER.trace("Window closed. Low and High watermark cleaned");
        windowOpened = false;
        previousHighWatermark = highWatermark;
        highWatermark = null;
        previousLowWatermark = lowWatermark;
        lowWatermark = null;
    }

    public boolean isTransactionVisible(Long eventTxId) {
        if (lowWatermark == null) {
            return true;
        }
        return eventTxId.compareTo(highWatermark.getXMin()) <= 0;
    }

    public boolean watermarksChanged() {
        LOGGER.trace("previousLowWatermark {}, lowWatermark {}, previousHighWatermark {}, highWatermark {}", previousLowWatermark, lowWatermark, previousHighWatermark,
                highWatermark);
        return !previousLowWatermark.equals(lowWatermark) || !previousHighWatermark.equals(highWatermark);
    }
}

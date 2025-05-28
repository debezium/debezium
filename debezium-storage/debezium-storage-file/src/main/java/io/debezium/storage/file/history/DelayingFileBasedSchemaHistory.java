/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.file.history;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryListener;

/**
 * A {@link io.debezium.relational.history.SchemaHistory} implementation that extends {@link FileSchemaHistory} and
 * adds configurable delays during recovery for testing purposes.
 */
@ThreadSafe
public final class DelayingFileBasedSchemaHistory extends FileSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayingFileBasedSchemaHistory.class);

    /**
     * The configuration property that specifies how long to delay when recovering each record.
     */
    public static final String RECOVERY_DELAY_MS_PROPERTY = CONFIGURATION_FIELD_PREFIX_STRING + "recovery.delay.ms";

    private long recoveryDelayMs = 0L;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        recoveryDelayMs = config.getLong(RECOVERY_DELAY_MS_PROPERTY, 0L);
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            for (HistoryRecord record : getRecords()) {
                records.accept(record);

                // Introduce a delay if configured
                if (recoveryDelayMs > 0) {
                    try {
                        LOGGER.info("Sleeping for {} ms to simulate recovery delay", recoveryDelayMs);
                        Thread.sleep(recoveryDelayMs);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while recovering records", e);
                    }
                }
            }
        });
    }

    @Override
    public String toString() {
        return "delaying-" + super.toString();
    }
}

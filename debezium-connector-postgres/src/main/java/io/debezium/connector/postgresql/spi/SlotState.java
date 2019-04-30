/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import io.debezium.annotation.Incubating;

/**
 * A simple data container that holds the state of the current slot
 */
@Incubating
public class SlotState {
    private final Long latestFlushedLsn;
    private final Long restartLsn;
    private final Long catalogXmin;
    private final boolean active;


    public SlotState(Long lastFlushLsn, Long restartLsn, Long catXmin, boolean active) {
        this.active = active;
        this.latestFlushedLsn = lastFlushLsn;
        this.restartLsn = restartLsn;
        this.catalogXmin = catXmin;
    }

    /**
     * @return the slot's `confirmed_flushed_lsn` value
     */
    public Long slotLastFlushedLsn() {
        return latestFlushedLsn;
    }

    /**
     * @return the slot's `restart_lsn` value
     */
    public Long slotRestartLsn() {
        return restartLsn;
    }

    /**
     * @return the slot's `catalog_xmin` value
     */
    public Long slotCatalogXmin() {
        return catalogXmin;
    }

    /**
     * @return if the slot is active
     */
    public boolean slotIsActive() {
        return active;
    }
}

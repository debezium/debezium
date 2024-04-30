/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.postgresql.connection.Lsn;

/**
 * A simple data container that holds the state of the current slot
 */
@Incubating
public class SlotState {
    private final Lsn latestFlushedLsn;
    private final Lsn restartLsn;
    private final Long catalogXmin;
    private final boolean active;
    private final Long restartCommitHT;

    public SlotState(Lsn lastFlushLsn, Lsn restartLsn, Long catXmin, boolean active, Long restartCommitHT) {
        this.active = active;
        this.latestFlushedLsn = lastFlushLsn;
        this.restartLsn = restartLsn;
        this.catalogXmin = catXmin;
        this.restartCommitHT = restartCommitHT;
    }

    /**
     * @return the slot's `confirmed_flushed_lsn` value
     */
    public Lsn slotLastFlushedLsn() {
        return latestFlushedLsn;
    }

    /**
     * @return the slot's `restart_lsn` value
     */
    public Lsn slotRestartLsn() {
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

    /**
     * @return if the slot's `yb_restart_commit_ht` value
     */
    public Long slotRestartCommitHT() {
        return restartCommitHT;
    }
}

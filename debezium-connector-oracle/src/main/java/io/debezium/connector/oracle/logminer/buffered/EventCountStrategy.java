/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A spill strategy that triggers spilling when a transaction exceeds a specified event count threshold.
 * This is the default strategy for determining when transactions should begin spilling to off-heap storage.
 *
 * @author Debezium Authors
 */
public class EventCountStrategy implements SpillStrategy {

    private final long spillThreshold;

    public EventCountStrategy(long spillThreshold) {
        this.spillThreshold = spillThreshold;
    }

    @Override
    public boolean shouldSpill(String txId, LogMinerEvent latest, TransactionView view) {
        return view.totalEventCount() >= spillThreshold;
    }

    public long getSpillThreshold() {
        return spillThreshold;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import java.util.List;
import java.util.Map;

import io.debezium.connector.oracle.logminer.buffered.AbstractCacheSlot;
import io.debezium.connector.oracle.logminer.buffered.AbstractLogMinerTransactionCache.LogMinerEventEntry;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

final class MemorySlot extends AbstractCacheSlot {
    MemoryTransaction transaction;
    List<LogMinerEventEntry> events;
    Map<Integer, LogMinerEvent> eventsByEventId;

    MemoryTransaction transaction() {
        return transaction;
    }

    List<LogMinerEventEntry> events() {
        return events;
    }

    @Override
    public void clear() {
        super.clear();
        transaction = null;
        events = null;
        eventsByEventId = null;
    }
}

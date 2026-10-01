/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

public abstract class AbstractCacheSlot extends Slot {
    LogMinerEvent lastEnqueuedEvent;
    Transaction deferredTransaction;

    LogMinerEvent lastEnqueuedEvent() {
        return lastEnqueuedEvent;
    }

    Transaction deferredTransaction() {
        return deferredTransaction;
    }

    @Override
    public void clear() {
        lastEnqueuedEvent = null;
        deferredTransaction = null;
    }
}

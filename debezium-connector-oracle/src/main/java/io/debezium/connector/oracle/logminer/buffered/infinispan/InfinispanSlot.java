/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import java.util.TreeSet;

import io.debezium.connector.oracle.logminer.buffered.Slot;

final class InfinispanSlot extends Slot {
    Long key;
    TreeSet<Integer> eventIds;

    TreeSet<Integer> eventIds() {
        return eventIds;
    }

    @Override
    public void clear() {
        super.clear();
        key = null;
        eventIds = null;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

/**
 * Conflict-free Replicated Data Types (CRDT)s. Unless otherwise noted, the implementations of these interfaces are not
 * thread-safe since they are expected to be used within a thread-safe process, and sent across a network to another thread-safe
 * process where they will be merged together.
 *
 * @author Randall Hauch
 */
public final class CRDT {

    /**
     * Create a new CRDT grow-only (G) counter. The operations on this counter are not threadsafe.
     *
     * @return the new counter; never null
     */
    public static GCounter newGCounter() {
        return new StateBasedGCounter();
    }

    /**
     * Create a new CRDT grow-only (G) counter pre-populated with the given value. The operations on this counter are not
     * threadsafe.
     *
     * @param adds the number of adds
     * @return the new counter; never null
     */
    public static GCounter newGCounter(long adds) {
        return new StateBasedGCounter(adds);
    }

    /**
     * Create a new CRDT positive and negative (PN) counter. The operations on this counter are not threadsafe.
     *
     * @return the new counter; never null
     */
    public static PNCounter newPNCounter() {
        return new StateBasedPNCounter();
    }

    /**
     * Create a new CRDT positive and negative (PN) counter pre-populated with the given values. The operations on this counter
     * are not threadsafe.
     *
     * @param adds the number of adds
     * @param removes the number of removes
     * @return the new counter; never null
     */
    public static PNCounter newPNCounter(long adds, long removes) {
        return new StateBasedPNCounter(adds, removes);
    }

    /**
     * Create a new CRDT positive and negative (PN) counter that records how much the value has changed since last reset. The
     * operations on this counter are not threadsafe.
     *
     * @return the new counter; never null
     */
    public static DeltaCounter newDeltaCounter() {
        return new StateBasedPNDeltaCounter();
    }

    /**
     * Create a new CRDT positive and negative (PN) counter that records how much the value has changed since last reset. The
     * operations on this counter are not threadsafe.
     *
     * @param totalAdds the total number of adds
     * @param totalRemoves the total number of removes
     * @param recentAdds the recent number of adds since last {@link DeltaCounter#reset()}
     * @param recentRemoves the recent number of removes since last {@link DeltaCounter#reset()}
     * @return the new counter; never null
     */
    public static DeltaCounter newDeltaCounter(long totalAdds, long totalRemoves, long recentAdds, long recentRemoves) {
        return new StateBasedPNDeltaCounter(totalAdds, totalRemoves, recentAdds, recentRemoves);
    }

    /**
     * Create a new CRDT positive and negative (PN) counter that records how much the value has changed since last reset. The
     * operations on this counter are not threadsafe.
     *
     * @param count the {@link DeltaCount} instance that should be used to pre-populate the new counter; may be null
     * @return the new counter; never null
     */
    public static DeltaCounter newDeltaCounter(DeltaCount count) {
        if (count == null) {
            return new StateBasedPNDeltaCounter();
        }
        return new StateBasedPNDeltaCounter(count.getIncrement(), count.getDecrement(),
                count.getChanges().getIncrement(), count.getChanges().getDecrement());
    }

    private CRDT() {
    }
}

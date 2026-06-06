/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

/**
 * A read-only result of the state of a {@link PNCounter}. The value may or may not change depending upon how the value was
 * obtained.
 */
public interface PNCount extends GCount {
    /**
     * Get the current value.
     *
     * @return the current value
     */
    @Override
    default long get() {
        return getIncrement() - getDecrement();
    }

    /**
     * Get the amount that the value decremented. The {@link #get() value} is the {@link #getIncrement() total
     * increments} minus the {@link #getDecrement() total decrements}
     *
     * @return the decremented value
     */
    long getDecrement();
}

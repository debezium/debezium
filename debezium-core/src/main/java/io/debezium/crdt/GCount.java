/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

/**
 * A read-only result of the state of a grow-only {@link GCounter}. The value may or may not change depending upon how the value
 * was obtained.
 */
public interface GCount extends Count {

    /**
     * Get the amount that the value incremented.
     *
     * @return the incremented value
     */
    long getIncrement();
}

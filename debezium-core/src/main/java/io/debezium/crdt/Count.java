/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.crdt;

/**
 * A read-only result of a counter. The value may or may not change depending upon how the value was obtained.
 */
public interface Count {
    /**
     * Get the current value.
     *
     * @return the current value
     */
    long get();
}

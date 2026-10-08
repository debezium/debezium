/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.memory;

import io.debezium.connector.oracle.logminer.buffered.Segments;

final class MemorySegments extends Segments<MemorySlot> {
    @Override
    protected MemorySlot[][] newSegments(int size) {
        return new MemorySlot[size][];
    }

    @Override
    protected MemorySlot[] newSegment(int size) {
        return new MemorySlot[size];
    }

    @Override
    protected MemorySlot newSlot() {
        return new MemorySlot();
    }
}

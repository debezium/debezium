/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan;

import io.debezium.connector.oracle.logminer.buffered.Segments;

final class InfinispanSegments extends Segments<InfinispanSlot> {
    @Override
    protected InfinispanSlot[][] newSegments(int size) {
        return new InfinispanSlot[size][];
    }

    @Override
    protected InfinispanSlot[] newSegment(int size) {
        return new InfinispanSlot[size];
    }

    @Override
    protected InfinispanSlot newSlot() {
        return new InfinispanSlot();
    }
}

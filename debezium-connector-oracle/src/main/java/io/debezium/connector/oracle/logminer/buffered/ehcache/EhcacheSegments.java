/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache;

import io.debezium.connector.oracle.logminer.buffered.Segments;

final class EhcacheSegments extends Segments<EhcacheSlot> {
    @Override
    protected EhcacheSlot[][] newSegments(int size) {
        return new EhcacheSlot[size][];
    }

    @Override
    protected EhcacheSlot[] newSegment(int size) {
        return new EhcacheSlot[size];
    }

    @Override
    protected EhcacheSlot newSlot() {
        return new EhcacheSlot();
    }
}

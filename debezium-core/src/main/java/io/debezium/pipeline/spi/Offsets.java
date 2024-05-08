/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.debezium.DebeziumException;

/**
 * Keeps track the source partitions to be processed by the connector task and their respective offsets.
 */
public final class Offsets<P extends Partition, O extends OffsetContext> implements Iterable<Entry<P, O>> {

    private final Map<P, O> offsets;

    private Offsets(Map<P, O> offsets) {
        this.offsets = offsets;
    }

    public static <P extends Partition, O extends OffsetContext> Offsets<P, O> of(P partition, O position) {
        Map<P, O> offsets = new HashMap<P, O>();
        offsets.put(partition, position);
        return new Offsets<>(offsets);
    }

    public static <P extends Partition, O extends OffsetContext> Offsets<P, O> of(Map<P, O> offsets) {
        return new Offsets<>(offsets);
    }

    public void resetOffset(P partition) {
        offsets.put(partition, null);
    }

    public Set<P> getPartitions() {
        return offsets.keySet();
    }

    public Map<P, O> getOffsets() {
        return offsets;
    }

    @Override
    public Iterator<Entry<P, O>> iterator() {
        return offsets.entrySet().iterator();
    }

    /**
     * Returns the offset of the only partition that the task is configured to use.
     *
     * This method is meant to be used only by the connectors that do not implement handling
     * multiple partitions per task.
     */
    public P getTheOnlyPartition() {
        if (offsets.size() != 1) {
            throw new DebeziumException("The task must be configured to use exactly one partition, "
                    + offsets.size() + " found");
        }

        return offsets.entrySet().iterator().next().getKey();
    }

    /**
     * Returns the offset of the only offset that the task is configured to use.
     *
     * This method is meant to be used only by the connectors that do not implement handling
     * multiple partitions per task.
     */
    public O getTheOnlyOffset() {
        if (offsets.size() != 1) {
            throw new DebeziumException("The task must be configured to use exactly one partition, "
                    + offsets.size() + " found");
        }

        return offsets.entrySet().iterator().next().getValue();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope.Operation;

/**
 * Statistics of {@link SourceRecord}s.
 *
 * @author Randall Hauch
 */
public class SourceRecordStats implements Consumer<SourceRecord> {

    private final AtomicLong EMPTY = new AtomicLong();
    private final EnumMap<Operation, AtomicLong> statsByOperation = new EnumMap<>(Operation.class);
    private final AtomicLong tombstones = new AtomicLong();

    /**
     * Create a new statistics object.
     */
    public SourceRecordStats() {
    }

    @Override
    public void accept(SourceRecord record) {
        if (record.value() == null) {
            tombstones.incrementAndGet();
        }
        else {
            Operation op = Envelope.operationFor(record);
            if (op != null) {
                statsByOperation.computeIfAbsent(op, key -> new AtomicLong()).incrementAndGet();
            }
        }
    }

    /**
     * Get the number of {@link #accept(SourceRecord) added} records that had the given {@link Operation}.
     *
     * @param op the operation for which the record count is to be returned
     * @return the count; never negative
     */
    public long numberOf(Operation op) {
        return statsByOperation.getOrDefault(op, EMPTY).get();
    }

    /**
     * Get the number of {@link Operation#CREATE CREATE} records that were {@link #accept(SourceRecord) added} to this object.
     *
     * @return the count; never negative
     */
    public long numberOfCreates() {
        return numberOf(Operation.CREATE);
    }

    /**
     * Get the number of {@link Operation#DELETE DELETE} records that were {@link #accept(SourceRecord) added} to this object.
     *
     * @return the count; never negative
     */
    public long numberOfDeletes() {
        return numberOf(Operation.DELETE);
    }

    /**
     * Get the number of {@link Operation#READ READ} records that were {@link #accept(SourceRecord) added} to this object.
     *
     * @return the count; never negative
     */
    public long numberOfReads() {
        return numberOf(Operation.READ);
    }

    /**
     * Get the number of {@link Operation#UPDATE UPDATE} records that were {@link #accept(SourceRecord) added} to this object.
     *
     * @return the count; never negative
     */
    public long numberOfUpdates() {
        return numberOf(Operation.UPDATE);
    }

    /**
     * Get the number of tombstone records that were {@link #accept(SourceRecord) added} to this object.
     *
     * @return the count; never negative
     */
    public long numberOfTombstones() {
        return tombstones.get();
    }

    /**
     * Reset all of the counters to 0.
     *
     * @return this object for method chaining purposes; never null
     */
    public SourceRecordStats reset() {
        this.statsByOperation.clear();
        this.tombstones.set(0);
        return this;
    }

    @Override
    public String toString() {
        return "" + numberOfCreates() + " creates, "
                + numberOfUpdates() + " updates, "
                + numberOfDeletes() + " deletes, "
                + numberOfReads() + " reads";
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.FunctionalReadWriteLock;

/**
 * A {@link DatabaseHistory} implementation that stores the schema history in a local file.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public final class MemoryDatabaseHistory extends AbstractDatabaseHistory {

    private final List<HistoryRecord> records = new ArrayList<>();
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();

    /**
     * Create an instance that keeps the history in memory.
     */
    public MemoryDatabaseHistory() {
    }

    @Override
    protected void storeRecord(HistoryRecord record) {
        lock.write(() -> records.add(record));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> this.records.forEach(records));
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        return !records.isEmpty();
    }

    @Override
    public String toString() {
        return "memory";
    }
}

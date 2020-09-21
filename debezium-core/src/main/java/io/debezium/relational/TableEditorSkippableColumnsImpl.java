/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allows for columns to be "missing" or "skipped" from the table to support captured_column_list for sql sever connector.
 * Example, columns 1,2,4 are populated, but 3 is excluded from the CDC tables.
 */
final class TableEditorSkippableColumnsImpl extends TableEditorImpl {

    /**
     * Trusts inputted column position to be correct.
     */
    @Override
    protected void add(Column defn) {
        if (defn != null) {
            getSortedColumns().put(defn.name().toLowerCase(), defn);
        }
        assert positionsAreValid();
    }

    /**
     * Do not automatically update columns.
     */
    @Override
    protected void updatePositions() {
        // Do nothing and allow columns to maintain their inputted positions.
    }

    /**
     * Verify columns are incrementing by some amount/unique.
     */
    @Override
    protected boolean positionsAreValid() {
        AtomicInteger position = new AtomicInteger(1);
        return getSortedColumns().values().stream().allMatch(defn -> defn.position() >= position.getAndSet(defn.position() + 1));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.concurrent.atomic.AtomicInteger;

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
        // Do nothing and allow
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

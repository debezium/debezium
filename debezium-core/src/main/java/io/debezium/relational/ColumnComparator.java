/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Comparator;

/**
 * A Comparator for {@link Column} instances and objects which create {@link Column) instances.
 *
 * @author Andreas Bergmeier
 */
final class ColumnComparator implements Comparator<Column> {

    @Override
    public int compare(Column lhs, Column rhs) {
        return lhs.compareTo(rhs);
    }

    public int compare(ColumnEditor lhs, Column rhs) {
        return lhs.create().compareTo(rhs);
    }

    public int compare(Column lhs, ColumnEditor rhs) {
        return lhs.compareTo(rhs.create());
    }
}
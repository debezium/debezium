/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;

/**
 * A utility class that contains various kinds of filters.
 * Currently only field-level filter is implemented (i.e. `field.exclude.list`).
 * Keyspace/table-level filters are not implemented.
 */
public class Filters {
    private final FieldFilterSelector fieldFilterSelector;

    public Filters(List<String> fieldExcludeList) {
        fieldFilterSelector = new FieldFilterSelector(fieldExcludeList);
    }

    /**
     * Get the field filter for a given table.
     */
    public FieldFilterSelector.FieldFilter getFieldFilter(KeyspaceTable table) {
        return fieldFilterSelector.selectFieldFilter(table);
    }
}

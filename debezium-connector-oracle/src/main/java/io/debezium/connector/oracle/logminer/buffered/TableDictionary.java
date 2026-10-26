/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.relational.TableId;

/**
 * Interns the {@link TableId} of stored events to an integer, so that every event of a table costs four bytes
 * for it rather than a reference to its own instance. One dictionary serves the whole cache; it only grows
 * with the number of distinct tables the connector sees.
 *
 * @author Chris Cranford
 */
public final class TableDictionary {

    public static final int NONE = -1;

    private final Map<TableId, Integer> idsByTable = new HashMap<>();
    private final List<TableId> tables = new ArrayList<>();

    /**
     * @return the table's number, or {@link #NONE} for {@code null}
     */
    public int intern(TableId tableId) {
        if (tableId == null) {
            return NONE;
        }

        Integer id = idsByTable.get(tableId);
        if (id == null) {
            id = tables.size();
            tables.add(tableId);
            idsByTable.put(tableId, id);
        }

        return id;
    }

    public TableId get(int id) {
        return id == NONE ? null : tables.get(id);
    }

    public int size() {
        return tables.size();
    }

    public void clear() {
        idsByTable.clear();
        tables.clear();
    }
}

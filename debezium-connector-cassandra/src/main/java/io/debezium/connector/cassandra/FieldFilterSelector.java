/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.ArrayList;
import java.util.List;

/**
 * This field filter selector is designed to determine the filter for excluding fields from a table.
 */
public class FieldFilterSelector {
    private final List<String> fieldExcludeList;

    FieldFilterSelector(List<String> fieldExcludeList) {
        this.fieldExcludeList = fieldExcludeList;
    }

    public interface FieldFilter {
        RowData apply(RowData rowData);
    }

    /**
     * Returns the field(s) filter for the given table.
     */
    public FieldFilter selectFieldFilter(KeyspaceTable keyspaceTable) {
        List<Field> filteredFields = new ArrayList<>();
        for (String column : fieldExcludeList) {
            Field field = new Field(column);
            if (field.keyspaceTable.equals(keyspaceTable)) {
                filteredFields.add(field);
            }
        }

        if (filteredFields.size() > 0) {
            return rowData -> {
                RowData copy = rowData.copy();
                for (Field field : filteredFields) {
                    if (copy.hasCell(field.column)) {
                        copy.removeCell(field.column);
                    }
                }
                return copy;
            };
        }
        else {
            return rowData -> rowData;
        }
    }

    /**
     * Representation of a fully qualified field, which has a {@link KeyspaceTable}
     * and a field name. Nested and repeated fields are not supported right now.
     */
    private static final class Field {
        final KeyspaceTable keyspaceTable;
        final String column;

        private Field(String fieldExcludeList) {
            String[] elements = fieldExcludeList.split("\\.", 3);
            String keyspace = elements[0];
            String table = elements[1];
            keyspaceTable = new KeyspaceTable(keyspace, table);
            column = elements[2];
        }
    }

}

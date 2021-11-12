/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Utility class for mapping columns to various data structures from from {@link Table} and {@link ResultSet}.
 */
public class ColumnUtils {

    public static MappedColumns toMap(Table table) {
        Map<String, Column> sourceTableColumns = new HashMap<>();
        int greatestColumnPosition = 0;
        for (Column column : table.columns()) {
            sourceTableColumns.put(column.name(), column);
            greatestColumnPosition = greatestColumnPosition < column.position()
                    ? column.position()
                    : greatestColumnPosition;
        }
        return new MappedColumns(sourceTableColumns, greatestColumnPosition);
    }

    public static ColumnArray toArray(ResultSet resultSet, Table table) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        Column[] columns = new Column[metaData.getColumnCount()];
        int greatestColumnPosition = 0;
        for (int i = 0; i < columns.length; i++) {
            columns[i] = table.columnWithName(metaData.getColumnName(i + 1));
            greatestColumnPosition = greatestColumnPosition < columns[i].position()
                    ? columns[i].position()
                    : greatestColumnPosition;
        }
        return new ColumnArray(columns, greatestColumnPosition);
    }

    public static class MappedColumns {

        private Map<String, Column> sourceTableColumns;
        private int greatestColumnPosition;

        public MappedColumns(Map<String, Column> sourceTableColumns, int greatestColumnPosition) {
            this.sourceTableColumns = sourceTableColumns;
            this.greatestColumnPosition = greatestColumnPosition;
        }

        public Map<String, Column> getSourceTableColumns() {
            return sourceTableColumns;
        }

        public int getGreatestColumnPosition() {
            return greatestColumnPosition;
        }
    }

    public static class ColumnArray {

        private Column[] columns;
        private int greatestColumnPosition;

        public ColumnArray(Column[] columns, int greatestColumnPosition) {
            this.columns = columns;
            this.greatestColumnPosition = greatestColumnPosition;
        }

        public Column[] getColumns() {
            return columns;
        }

        public int getGreatestColumnPosition() {
            return greatestColumnPosition;
        }
    }

    private ColumnUtils() {
    }
}

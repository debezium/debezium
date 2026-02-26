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
 * Utility class for mapping columns to various data structures from {@link Table} and {@link ResultSet}.
 */
public class ColumnUtils {

    public static MappedColumns toMap(Table table) {
        Map<String, Column> sourceTableColumns = new HashMap<>();
        int greatestColumnPosition = 0;
        for (Column column : table.columns()) {
            sourceTableColumns.put(column.name(), column);
            greatestColumnPosition = Math.max(greatestColumnPosition, column.position());
        }
        return new MappedColumns(sourceTableColumns, greatestColumnPosition);
    }

    public static ColumnArray toArray(ResultSet resultSet, Table table) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        Column[] columns = new Column[metaData.getColumnCount()];
        int greatestColumnPosition = 0;
        for (int i = 0; i < columns.length; i++) {
            final String columnName = metaData.getColumnName(i + 1);
            columns[i] = table.columnWithName(columnName);
            if (columns[i] == null) {
                // This situation can happen when SQL Server and Db2 schema is changed before
                // an incremental snapshot is started and no event with the new schema has been
                // streamed yet.
                // This warning will help to identify the issue in case of a support request.

                final String[] resultSetColumns = new String[metaData.getColumnCount()];
                for (int j = 0; j < metaData.getColumnCount(); j++) {
                    resultSetColumns[j] = metaData.getColumnName(j + 1);
                }
                throw new IllegalArgumentException("Column '"
                        + columnName
                        + "' not found in result set '"
                        + String.join(", ", resultSetColumns)
                        + "' for table '"
                        + table.id()
                        + "', "
                        + table
                        + ". This might be caused by DBZ-4350");
            }
            greatestColumnPosition = Math.max(greatestColumnPosition, columns[i].position());
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

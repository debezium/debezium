/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;

/**
 * The logical representation of a position for the change in the transaction log.
 * During each sourcing cycle it is necessary to query all change tables and then
 * make a total order of changes across all tables.<br>
 * This class represents an open database cursor over the change table that is
 * able to move the cursor forward and report the LSN for the change to which the cursor
 * now points.
 *
 * @author Jiri Pechanec
 *
 */
public class ChangeTablePointer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTablePointer.class);

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private final ChangeTable changeTable;
    private final ResultSet resultSet;
    private final JdbcConnection.ResultSetMapper<Object[]> resultSetMapper;
    private boolean completed = false;
    private TxLogPosition currentChangePosition;

    public ChangeTablePointer(ChangeTable changeTable, ResultSet resultSet) throws SQLException {
        this.changeTable = changeTable;
        this.resultSet = resultSet;
        this.resultSetMapper = createResultSetMapper(changeTable.getSourceTable());
    }

    public ChangeTable getChangeTable() {
        return changeTable;
    }

    public TxLogPosition getChangePosition() {
        return currentChangePosition;
    }

    public int getOperation() throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    public Object[] getData() throws SQLException {
        return resultSetMapper.apply(resultSet);
    }

    public boolean next() throws SQLException {
        completed = !resultSet.next();
        currentChangePosition = completed ? TxLogPosition.NULL : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
        if (completed) {
            LOGGER.trace("Closing result set of change tables for table {}", changeTable);
            resultSet.close();
        }
        return !completed;
    }

    public boolean isCompleted() {
        return completed;
    }

    public int compareTo(ChangeTablePointer o) {
        return getChangePosition().compareTo(o.getChangePosition());
    }

    @Override
    public String toString() {
        return "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed="
                + completed + ", currentChangePosition=" + currentChangePosition + "]";
    }

    /**
     * Internally each row is represented as an array of objects, where the order of values
     * corresponds to the order of columns (fields) in the table schema. However, when capture
     * instance contains only a subset of original's table column, in order to preserve the
     * aforementioned order of values in array, raw database results have to be adjusted
     * accordingly.
     *
     * @param table original table
     * @return a mapper which adjusts order of values in case the capture instance contains only
     * a subset of columns
     */
    private JdbcConnection.ResultSetMapper<Object[]> createResultSetMapper(Table table) throws SQLException {
        final List<String> sourceTableColumns = table.columnNames();
        final List<String> resultColumns = getResultColumnNames();
        final int sourceColumnCount = sourceTableColumns.size();
        final int resultColumnCount = resultColumns.size();

        if (sourceTableColumns.equals(resultColumns)) {
            return resultSet -> {
                final Object[] data = new Object[sourceColumnCount];
                for (int i = 0; i < sourceColumnCount; i++) {
                    data[i] = resultSet.getObject(COL_DATA + i);
                }
                return data;
            };
        }
        else {
            final IndicesMapping indicesMapping = new IndicesMapping(sourceTableColumns, resultColumns);
            return resultSet -> {
                final Object[] data = new Object[sourceColumnCount];
                for (int i = 0; i < resultColumnCount; i++) {
                    int index = indicesMapping.getSourceTableColumnIndex(i);
                    data[index] = resultSet.getObject(COL_DATA + i);
                }
                return data;
            };
        }
    }

    private List<String> getResultColumnNames() throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount() - (COL_DATA - 1);
        final List<String> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            columns.add(resultSet.getMetaData().getColumnName(COL_DATA + i));
        }
        return columns;
    }

    private class IndicesMapping {

        private final Map<Integer, Integer> mapping;

        IndicesMapping(List<String> sourceTableColumns, List<String> captureInstanceColumns) {
            this.mapping = new HashMap<>(sourceTableColumns.size());

            for (int i = 0; i < captureInstanceColumns.size(); ++i) {
                mapping.put(i, sourceTableColumns.indexOf(captureInstanceColumns.get(i)));
            }

        }

        int getSourceTableColumnIndex(int resultCaptureInstanceColumnIndex) {
            return mapping.get(resultCaptureInstanceColumnIndex);
        }
    }
}

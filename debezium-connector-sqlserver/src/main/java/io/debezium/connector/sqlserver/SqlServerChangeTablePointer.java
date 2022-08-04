/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.ColumnUtils;

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
public class SqlServerChangeTablePointer extends ChangeTableResultSet<SqlServerChangeTable, TxLogPosition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeTablePointer.class);

    private static final int INVALID_COLUMN_INDEX = -1;

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private ResultSetMapper<Object[]> resultSetMapper;
    private final ResultSet resultSet;
    private final int columnDataOffset;

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet) {
        super(changeTable, resultSet, COL_DATA);
        // Store references to these because we can't get them from our superclass
        this.resultSet = resultSet;
        this.columnDataOffset = COL_DATA;
    }

    protected ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    protected int getOperation(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    @Override
    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        if (resultSet.getMetaData().getColumnType(columnIndex) == Types.TIME) {
            return resultSet.getTime(columnIndex);
        }
        return super.getColumnData(resultSet, columnIndex);
    }

    @Override
    protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
        return isCompleted() ? TxLogPosition.NULL
                : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
    }

    /**
     * Check whether TX in currentChangePosition is newer (higher) than TX in previousChangePosition
     * @return true <=> TX in currentChangePosition > TX in previousChangePosition
     * @throws SQLException
     */
    protected boolean isNewTransaction() throws SQLException {
        return (getPreviousChangePosition() != null) &&
                getChangePosition().getCommitLsn().compareTo(getPreviousChangePosition().getCommitLsn()) > 0;
    }

    @Override
    public Object[] getData() throws SQLException {
        if (resultSetMapper == null) {
            this.resultSetMapper = createResultSetMapper(getChangeTable().getSourceTable());
        }
        return resultSetMapper.apply(resultSet);
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
    private ResultSetMapper<Object[]> createResultSetMapper(Table table) throws SQLException {
        ColumnUtils.MappedColumns columnMap = ColumnUtils.toMap(table);
        final ResultSetMetaData rsmd = resultSet.getMetaData();
        final int columnCount = rsmd.getColumnCount() - columnDataOffset;
        final List<String> resultColumns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            resultColumns.add(rsmd.getColumnName(columnDataOffset + i));
        }
        final int resultColumnCount = resultColumns.size();

        final IndicesMapping indicesMapping = new IndicesMapping(columnMap.getSourceTableColumns(), resultColumns);
        return resultSet -> {
            final Object[] data = new Object[columnMap.getGreatestColumnPosition()];
            for (int i = 0; i < resultColumnCount; i++) {
                int index = indicesMapping.getSourceTableColumnIndex(i);
                if (index == INVALID_COLUMN_INDEX) {
                    LOGGER.trace("Data for table '{}' contains a column without position mapping", table.id());
                    continue;
                }
                data[index] = getColumnData(resultSet, columnDataOffset + i);
            }
            return data;
        };
    }

    private class IndicesMapping {

        private final Map<Integer, Integer> mapping;

        IndicesMapping(Map<String, Column> sourceTableColumns, List<String> captureInstanceColumns) {
            this.mapping = new HashMap<>(sourceTableColumns.size(), 1.0F);

            for (int i = 0; i < captureInstanceColumns.size(); ++i) {
                final String columnName = captureInstanceColumns.get(i);
                final Column column = sourceTableColumns.get(columnName);
                if (column == null) {
                    LOGGER.warn("Column '{}' available in capture table not found among source table columns", columnName);
                    mapping.put(i, INVALID_COLUMN_INDEX);
                }
                else {
                    mapping.put(i, column.position() - 1);
                }
            }

        }

        int getSourceTableColumnIndex(int resultCaptureInstanceColumnIndex) {
            return mapping.get(resultCaptureInstanceColumnIndex);
        }
    }

}

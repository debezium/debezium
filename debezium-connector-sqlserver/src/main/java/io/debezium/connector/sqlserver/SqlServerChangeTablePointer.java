/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.pipeline.source.spi.ChangeTableResultSet;
import io.debezium.relational.Column;
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
public class SqlServerChangeTablePointer extends ChangeTableResultSet<SqlServerChangeTable, TxLogPosition> {

    // private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeTablePointer.class);

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private ResultSetMapper<Object[]> resultSetMapper;
    // private SqlServerChangeTable changeTable;
    private ResultSet resultSet;
    private int columnDataOffset;

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet) {
        this(changeTable, resultSet, COL_DATA);
    }

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet, int columnDataOffset) {
        super(changeTable, resultSet, columnDataOffset);
        // Store references to these because we can't get them from our superclass
        // this.changeTable = changeTable;
        this.resultSet = resultSet;
        this.columnDataOffset = columnDataOffset;
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
        // final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (columnDataOffset - 1);
        // final Object[] data = new Object[dataColumnCount];
        // for (int i = 0; i < dataColumnCount; ++i) {
        // data[i] = getColumnData(resultSet, columnDataOffset + i);
        // }
        // return data;
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
        final List<String> sourceTableColumns = table.columns().stream()
                .map(Column::name)
                .collect(Collectors.toList());
        final List<String> resultColumns = getResultColumnNames();
        final int sourceColumnCount = sourceTableColumns.size();
        final int resultColumnCount = resultColumns.size();

        if (sourceTableColumns.equals(resultColumns)) {
            return resultSet -> {
                final Object[] data = new Object[sourceColumnCount];
                for (int i = 0; i < sourceColumnCount; i++) {
                    data[i] = getColumnData(resultSet, columnDataOffset + i);
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
                    data[index] = getColumnData(resultSet, columnDataOffset + i);
                }
                return data;
            };
        }
    }

    private List<String> getResultColumnNames() throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount() - (columnDataOffset - 1);
        final List<String> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; ++i) {
            columns.add(resultSet.getMetaData().getColumnName(columnDataOffset + i));
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

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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
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
    private static final int COL_UPDATE_MASK = 4;
    private static final int COL_COMMAND_ID = 5;
    private static final int COL_DATA = 6;

    private ResultSetMapper<Object[]> resultSetMapper;
    private final SqlServerConnection connection;
    private final Lsn fromLsn;
    private final Lsn toLsn;
    private final int maxRowsPerResultSet;
    // Distinguishes the completed-cursor sentinel (NULL vs NULL_LEGACY).
    private final boolean directMode;
    private final TxLogPosition resumeFromPosition;

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, SqlServerConnection connection, Lsn fromLsn, Lsn toLsn,
                                       TxLogPosition resumeFromPosition, int maxRowsPerResultSet,
                                       SqlServerConnectorConfig.DataQueryMode dataQueryMode) {
        super(changeTable, COL_DATA, maxRowsPerResultSet);
        this.directMode = dataQueryMode == SqlServerConnectorConfig.DataQueryMode.DIRECT;
        this.connection = connection;
        this.fromLsn = fromLsn;
        this.toLsn = toLsn;
        this.resumeFromPosition = resumeFromPosition;
        this.maxRowsPerResultSet = maxRowsPerResultSet;
    }

    @Override
    protected int getOperation(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    @Override
    protected Object getColumnData(ResultSet resultSet, int columnIndex) throws SQLException {
        int columnType = resultSet.getMetaData().getColumnType(columnIndex);
        if (columnType == Types.TIME) {
            return resultSet.getTimestamp(columnIndex);
        }
        else if (columnType == Types.TIMESTAMP) {
            return resultSet.getObject(columnIndex, LocalDateTime.class);
        }
        return super.getColumnData(resultSet, columnIndex);
    }

    @Override
    protected TxLogPosition getNextChangePosition(ResultSet resultSet) throws SQLException {
        return isCompleted() ? (directMode ? TxLogPosition.NULL : TxLogPosition.NULL_LEGACY)
                : TxLogPosition.valueOf(
                        Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)),
                        Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)),
                        resultSet.getInt(COL_OPERATION),
                        commandId(resultSet));
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
    protected ResultSet getNextResultSet(TxLogPosition lastPositionSeen) throws SQLException {
        // next page in a running connector
        if (!isEmptyPosition(lastPositionSeen)) {
            return connection.getChangesForTable(getChangeTable(), lastPositionSeen.getCommitLsn(), lastPositionSeen.getInTxLsn(),
                    lastPositionSeen.getOperation(), lastPositionSeen.getCommandId(), toLsn, maxRowsPerResultSet);
        }

        // restarted connector which has not seen any position yet
        if (resumeFromPosition != null && fromLsn.equals(resumeFromPosition.getCommitLsn())) {
            return connection.getChangesForTable(getChangeTable(), fromLsn, resumeFromPosition.getInTxLsn(), 0,
                    resumeFromPosition.getCommandId(), toLsn, maxRowsPerResultSet);
        }

        // running connector with new range.
        return connection.getChangesForTable(getChangeTable(), fromLsn, Lsn.ZERO, 0, -1, toLsn, maxRowsPerResultSet);
    }

    private static boolean isEmptyPosition(TxLogPosition position) {
        return position == null || position.equals(TxLogPosition.NULL) || position.equals(TxLogPosition.NULL_LEGACY);
    }

    private static Integer commandId(ResultSet resultSet) throws SQLException {
        int commandId = resultSet.getInt(COL_COMMAND_ID);
        return resultSet.wasNull() ? null : commandId;
    }

    @Override
    public Object[] getData() throws SQLException {
        if (resultSetMapper == null) {
            this.resultSetMapper = createResultSetMapper(getChangeTable().getSourceTable());
        }
        return resultSetMapper.apply(getResultSet());
    }

    /**
     * Internally each row is represented as an array of objects, where the order of values
     * corresponds to the order of columns (fields) in the table schema. However, when capture
     * instance contains only a subset of original's table column, in order to preserve the
     * aforementioned order of values in array, raw database results have to be adjusted
     * accordingly.
     *
     * <p>For UPDATE operations, max-type columns ({@code varchar(max)}, {@code nvarchar(max)},
     * {@code varbinary(max)}) that were not modified are stored as NULL in the CDC capture table.
     * This method uses the {@code __$update_mask} bitmask to detect such columns and replaces
     * their NULL values with {@link SqlServerValueConverters#UNAVAILABLE_VALUE} so that they
     * are emitted using the configured {@code unavailable.value.placeholder}.
     *
     * @param table original table
     * @return a mapper which adjusts order of values in case the capture instance contains only
     * a subset of columns
     */
    private ResultSetMapper<Object[]> createResultSetMapper(Table table) throws SQLException {
        ColumnUtils.MappedColumns columnMap = ColumnUtils.toMap(table);
        final ResultSetMetaData rsmd = getResultSet().getMetaData();
        final int columnCount = rsmd.getColumnCount() - COL_DATA;
        final List<String> resultColumns = new ArrayList<>(columnCount);
        final boolean[] maxColumns = new boolean[columnCount];
        boolean hasAnyMaxColumn = false;
        for (int i = 0; i < columnCount; ++i) {
            final String columnName = rsmd.getColumnName(COL_DATA + i);
            resultColumns.add(columnName);
            final Column column = columnMap.getSourceTableColumns().get(columnName);
            if (column != null) {
                maxColumns[i] = SqlServerDatabaseSchema.isMaxColumn(column);
            }
            else {
                final int jdbcType = rsmd.getColumnType(COL_DATA + i);
                maxColumns[i] = SqlServerDatabaseSchema.isMaxColumnJdbcType(jdbcType);
            }
            if (maxColumns[i]) {
                hasAnyMaxColumn = true;
            }
        }
        final int resultColumnCount = resultColumns.size();
        final boolean checkUpdateMask = hasAnyMaxColumn;

        final IndicesMapping indicesMapping = new IndicesMapping(columnMap.getSourceTableColumns(), resultColumns);
        return resultSet -> {
            final Object[] data = new Object[columnMap.getGreatestColumnPosition()];

            final byte[] updateMask;
            final int operation;
            if (checkUpdateMask) {
                operation = resultSet.getInt(COL_OPERATION);
                updateMask = resultSet.getBytes(COL_UPDATE_MASK);
            }
            else {
                operation = 0;
                updateMask = null;
            }

            for (int i = 0; i < resultColumnCount; i++) {
                int index = indicesMapping.getSourceTableColumnIndex(i);
                if (index == INVALID_COLUMN_INDEX) {
                    LOGGER.trace("Data for table '{}' contains a column without position mapping", table.id());
                    continue;
                }
                data[index] = getColumnData(resultSet, COL_DATA + i);

                if (maxColumns[i] && data[index] == null
                        && isUpdateOperation(operation)
                        && !isColumnChanged(updateMask, i)) {
                    LOGGER.trace("Column at index {} for table '{}' was not changed in UPDATE, replacing with unavailable value placeholder",
                            i, table.id());
                    data[index] = SqlServerValueConverters.UNAVAILABLE_VALUE;
                }
            }
            return data;
        };
    }

    /**
     * Check whether the given operation is an UPDATE (before or after image).
     *
     * @param operation the CDC operation code
     * @return {@code true} if the operation is an UPDATE
     */
    private static boolean isUpdateOperation(int operation) {
        return operation == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE
                || operation == SqlServerChangeRecordEmitter.OP_UPDATE_AFTER;
    }

    /**
     * Check whether a column was changed based on the CDC {@code __$update_mask} bitmask.
     *
     * <p>The update mask is a {@code varbinary} value with one bit per captured column, where a bit
     * value of 1 indicates the column was modified. The column with ordinal 1 is the lowest bit of
     * the <em>last</em> byte of the mask and higher ordinals continue towards its first byte, the
     * same layout {@code sys.fn_cdc_is_bit_set} reads with
     * {@code SUBSTRING(mask, DATALENGTH(mask) - (ordinal - 1) / 8, 1)}.
     *
     * @param updateMask the raw update mask bytes from the CDC result set
     * @param columnIndex the 0-based index of the column in the captured column list
     * @return {@code true} if the column was changed, or if the mask is unavailable
     */
    @VisibleForTesting
    static boolean isColumnChanged(byte[] updateMask, int columnIndex) {
        if (updateMask == null) {
            return true;
        }
        final int byteIndex = updateMask.length - 1 - columnIndex / 8;
        final int bitIndex = columnIndex % 8;
        if (byteIndex < 0) {
            return true;
        }
        return (updateMask[byteIndex] & (1 << bitIndex)) != 0;
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.util.Map;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;

/**
 * Emits change data based on a single {@link RowLCR} event.
 *
 * @author Gunnar Morling
 */
public class XStreamChangeRecordEmitter extends BaseChangeRecordEmitter<ColumnValue> {

    private final RowLCR lcr;

    public XStreamChangeRecordEmitter(OracleConnectorConfig connectorConfig, Partition partition, OffsetContext offset, RowLCR lcr,
                                      Map<String, Object> oldChunkValues, Map<String, Object> newChunkValues,
                                      Table table, OracleDatabaseSchema schema, Clock clock) {
        super(connectorConfig, partition, offset, schema, table, clock,
                buildOldColumnValues(table, lcr, oldChunkValues, newChunkValues),
                buildNewColumnValues(table, lcr, newChunkValues));
        this.lcr = lcr;
    }

    private static Object[] buildNewColumnValues(Table table, RowLCR lcr, Map<String, Object> newChunkValues) {
        // For LOB_WRITE/LOB_TRIM/LOB_ERASE, Oracle XStream typically carries only the LOB
        // column in newValues; the primary key and other unchanged columns live in oldValues.
        // Merge oldValues as a baseline so those columns are populated in the emitted record.
        if (isLobOperation(lcr)) {
            return mergeColumnValues(table, lcr.getOldValues(), lcr.getNewValues(), newChunkValues);
        }
        return getColumnValues(table, lcr.getNewValues(), newChunkValues);
    }

    private static Object[] buildOldColumnValues(Table table, RowLCR lcr, Map<String, Object> oldChunkValues,
                                                 Map<String, Object> newChunkValues) {
        // For LOB ops, oldValues from XStream is typically empty (no PK, no other columns).
        // If we build old-state from oldValues alone, the emitted oldKey has a null primary key
        // while newKey has the real PK, triggering Debezium's PK-change-update path which emits
        // a DELETE + CREATE pair (op=c) instead of a regular UPDATE (op=u).
        //
        // Since LOB operations don't change non-LOB columns, reuse the merged new-state values
        // as a baseline and overlay oldChunkValues (UNAVAILABLE_VALUE markers) so the `before`
        // payload correctly reports the LOB columns as unavailable without claiming they held
        // the post-op value.
        if (isLobOperation(lcr)) {
            Object[] values = mergeColumnValues(table, lcr.getOldValues(), lcr.getNewValues(), newChunkValues);
            for (Map.Entry<String, Object> entry : oldChunkValues.entrySet()) {
                final int index = table.columnWithName(entry.getKey()).position() - 1;
                values[index] = entry.getValue();
            }
            return values;
        }
        return getColumnValues(table, lcr.getOldValues(), oldChunkValues);
    }

    private static boolean isLobOperation(RowLCR lcr) {
        final String cmd = lcr.getCommandType();
        return RowLCR.LOB_WRITE.equals(cmd) || RowLCR.LOB_TRIM.equals(cmd) || RowLCR.LOB_ERASE.equals(cmd);
    }

    @Override
    public Operation getOperation() {
        switch (lcr.getCommandType()) {
            case RowLCR.INSERT:
                return Operation.CREATE;
            case RowLCR.DELETE:
                return Operation.DELETE;
            case RowLCR.UPDATE:
            case RowLCR.LOB_WRITE:
            case RowLCR.LOB_TRIM:
            case RowLCR.LOB_ERASE:
                return Operation.UPDATE;
            case "TRUNCATE TABLE":
                return Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + lcr);
        }
    }

    private static Object[] getColumnValues(Table table, ColumnValue[] columnValues, Map<String, Object> chunkValues) {
        Object[] values = new Object[table.columns().size()];
        overlayColumnValues(values, table, columnValues);

        // Overlay chunk values into non-chunk value array
        for (Map.Entry<String, Object> entry : chunkValues.entrySet()) {
            final int index = table.columnWithName(entry.getKey()).position() - 1;
            values[index] = entry.getValue();
        }

        return values;
    }

    /**
     * Builds the new-state values array for a LOB_WRITE/LOB_TRIM/LOB_ERASE LCR.
     * Uses {@code oldValues} as a baseline (so primary-key and other unchanged
     * columns are populated) and overlays {@code newValues} (the LOB column(s)
     * the operation actually modified) plus any reselected LOB values carried
     * in {@code chunkValues}.
     */
    private static Object[] mergeColumnValues(Table table, ColumnValue[] oldValues,
                                              ColumnValue[] newValues, Map<String, Object> chunkValues) {
        Object[] values = new Object[table.columns().size()];
        overlayColumnValues(values, table, oldValues);
        overlayColumnValues(values, table, newValues);
        for (Map.Entry<String, Object> entry : chunkValues.entrySet()) {
            final int index = table.columnWithName(entry.getKey()).position() - 1;
            values[index] = entry.getValue();
        }
        return values;
    }

    private static void overlayColumnValues(Object[] values, Table table, ColumnValue[] columnValues) {
        if (columnValues == null) {
            return;
        }
        for (ColumnValue columnValue : columnValues) {
            // Skip Oracle ROW_ARCHIVAL column
            if ("ORA_ARCHIVE_STATE".equals(columnValue.getColumnName())) {
                continue;
            }
            int index = table.columnWithName(columnValue.getColumnName()).position() - 1;
            values[index] = columnValue.getColumnData();
        }
    }
}

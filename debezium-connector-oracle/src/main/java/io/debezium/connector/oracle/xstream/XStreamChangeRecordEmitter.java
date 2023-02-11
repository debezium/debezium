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
        super(connectorConfig, partition, offset, schema, table, clock, getColumnValues(table, lcr.getOldValues(), oldChunkValues),
                getColumnValues(table, lcr.getNewValues(), newChunkValues));
        this.lcr = lcr;
    }

    @Override
    public Operation getOperation() {
        switch (lcr.getCommandType()) {
            case RowLCR.INSERT:
                return Operation.CREATE;
            case RowLCR.DELETE:
                return Operation.DELETE;
            case RowLCR.UPDATE:
                return Operation.UPDATE;
            case "TRUNCATE TABLE":
                return Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + lcr);
        }
    }

    private static Object[] getColumnValues(Table table, ColumnValue[] columnValues, Map<String, Object> chunkValues) {
        Object[] values = new Object[table.columns().size()];
        if (columnValues != null) {
            for (ColumnValue columnValue : columnValues) {
                int index = table.columnWithName(columnValue.getColumnName()).position() - 1;
                values[index] = columnValue.getColumnData();
            }
        }

        // Overlay chunk values into non-chunk value array
        for (Map.Entry<String, Object> entry : chunkValues.entrySet()) {
            final int index = table.columnWithName(entry.getKey()).position() - 1;
            values[index] = entry.getValue();
        }

        return values;
    }
}

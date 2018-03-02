/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;

/**
 * Emits change data based on a single {@link RowLCR} event.
 *
 * @author Gunnar Morling
 */
public class OracleChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final RowLCR lcr;
    private final Table table;

    public OracleChangeRecordEmitter(OffsetContext offset, RowLCR lcr, Table table, Clock clock) {
        super(offset, clock);

        this.lcr = lcr;
        this.table = table;
    }

    @Override
    protected Operation getOperation() {
        switch(lcr.getCommandType()) {
            case RowLCR.INSERT: return Operation.CREATE;
            case RowLCR.DELETE: return Operation.DELETE;
            case RowLCR.UPDATE: return Operation.UPDATE;
            default: throw new IllegalArgumentException("Received event of unexpected command type: " + lcr);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return getColumnValues(lcr.getOldValues());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return getColumnValues(lcr.getNewValues());
    }

    private Object[] getColumnValues(ColumnValue[] columnValues) {
        Object[] values = new Object[table.columnNames().size()];

        for (ColumnValue columnValue : columnValues) {
            int index = table.columnNames().indexOf(columnValue.getColumnName());
            values[index] = columnValue.getColumnData();
        }

        return values;
    }
}

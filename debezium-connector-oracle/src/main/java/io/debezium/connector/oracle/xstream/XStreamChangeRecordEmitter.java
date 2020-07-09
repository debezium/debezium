/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
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

    public XStreamChangeRecordEmitter(OffsetContext offset, RowLCR lcr, Table table, Clock clock) {
        super(offset, table, clock);
        this.lcr = lcr;
    }

    @Override
    protected Operation getOperation() {
        switch (lcr.getCommandType()) {
            case RowLCR.INSERT:
                return Operation.CREATE;
            case RowLCR.DELETE:
                return Operation.DELETE;
            case RowLCR.UPDATE:
                return Operation.UPDATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + lcr);
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

    @Override
    protected String getColumnName(ColumnValue columnValue) {
        return columnValue.getColumnName();
    }

    @Override
    protected Object getColumnData(ColumnValue columnValue) {
        return columnValue.getColumnData();
    }
}

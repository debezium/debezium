/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change record based on a single {@link LogMinerDmlEntry} event.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final int operation;
    private final Object[] oldValues;
    private final Object[] newValues;

    public LogMinerChangeRecordEmitter(OffsetContext offset, int operation, Object[] oldValues,
                                       Object[] newValues, Table table, Clock clock) {
        super(offset, table, clock);
        this.operation = operation;
        this.oldValues = oldValues;
        this.newValues = newValues;
    }

    @Override
    protected Operation getOperation() {
        switch (operation) {
            case RowMapper.INSERT:
                return Operation.CREATE;
            case RowMapper.UPDATE:
            case RowMapper.SELECT_LOB_LOCATOR:
                return Operation.UPDATE;
            case RowMapper.DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + operation);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oldValues;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return newValues;
    }
}

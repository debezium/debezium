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

/**
 * Base class to emit change data based on a single entry event.
 */
public abstract class BaseChangeRecordEmitter<T> extends RelationalChangeRecordEmitter {

    protected final Table table;

    protected BaseChangeRecordEmitter(OffsetContext offset, Table table, Clock clock) {
        super(offset, clock);
        this.table = table;
    }

    abstract protected Operation getOperation();

    abstract protected String getColumnName(T columnValue);

    abstract protected Object getColumnData(T columnValue);

    protected Object[] getColumnValues(T[] columnValues) {
        Object[] values = new Object[table.columns().size()];

        for (T columnValue : columnValues) {
            int index = table.columnWithName(getColumnName(columnValue)).position() - 1;
            values[index] = getColumnData(columnValue);
        }

        return values;
    }
}

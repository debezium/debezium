/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import oracle.streams.ColumnValue;
import oracle.streams.RowLCR;

// TODO extract RelationalChangeRecordEmitter

public class OracleChangeRecordEmitter implements ChangeRecordEmitter {

    private final RowLCR lcr;
    private final Table table;
    private final Clock clock;

    public OracleChangeRecordEmitter(RowLCR lcr, Table table, Clock clock) {
        this.lcr = lcr;
        this.table = table;
        this.clock = clock;
    }

    @Override
    public void emitChangeRecords(OffsetContext offsetContext, DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        TableSchema tableSchema = (TableSchema) schema;
        Operation operation = getOperation();

        if (operation == Operation.CREATE) {
            Object[] columnValues = getColumnValues(lcr.getNewValues());
            Object key = tableSchema.keyFromColumnData(columnValues);
            Struct value = tableSchema.valueFromColumnData(columnValues);
            Struct envelope = tableSchema.getEnvelopeSchema().create(value, offsetContext.getSourceInfo(), clock.currentTimeInMillis());

            receiver.changeRecord(operation, key, envelope, offsetContext);

        }
        else if (operation == Operation.UPDATE) {
            throw new UnsupportedOperationException("Not yet implemented");
            // TODO handle PK change
        }
        else if (operation == Operation.DELETE) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    private Operation getOperation() {
        switch(lcr.getCommandType()) {
            case "INSERT": return Operation.CREATE;
            case "DELETE": return Operation.DELETE;
            case "UPDATE": return Operation.UPDATE;
            default: throw new IllegalArgumentException("Received event of unexpected command type: " + lcr);
        }
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

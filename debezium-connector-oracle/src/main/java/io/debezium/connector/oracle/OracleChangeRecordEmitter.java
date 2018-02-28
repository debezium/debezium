/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Objects;

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

        switch(operation) {
            case CREATE:
                emitCreateRecord(offsetContext, receiver, tableSchema, operation);
                break;
            case UPDATE:
                emitUpdateRecord(offsetContext, receiver, tableSchema, operation);
                break;
            case DELETE:
                emitDeleteRecord(offsetContext, receiver, tableSchema, operation);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    private void emitCreateRecord(OffsetContext offsetContext, Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] columnValues = getColumnValues(lcr.getNewValues());
        Object key = tableSchema.keyFromColumnData(columnValues);
        Struct value = tableSchema.valueFromColumnData(columnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(value, offsetContext.getSourceInfo(), clock.currentTimeInMillis());

        receiver.changeRecord(operation, key, envelope, offsetContext);
    }

    private void emitUpdateRecord(OffsetContext offsetContext, Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] newColumnValues = getColumnValues(lcr.getNewValues());
        Object[] oldColumnValues = getColumnValues(lcr.getOldValues());

        Object oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Object newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        // regular update
        if (Objects.equals(oldKey, newKey)) {
            Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());
            receiver.changeRecord(operation, newKey, envelope, offsetContext);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());
            receiver.changeRecord(Operation.DELETE, oldKey, envelope, offsetContext);

            envelope = tableSchema.getEnvelopeSchema().create(newValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());
            receiver.changeRecord(operation, oldKey, envelope, offsetContext);
        }
    }

    private void emitDeleteRecord(OffsetContext offsetContext, Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] oldColumnValues = getColumnValues(lcr.getOldValues());
        Object oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());
        receiver.changeRecord(operation, oldKey, envelope, offsetContext);
    }

    private Operation getOperation() {
        switch(lcr.getCommandType()) {
            case RowLCR.INSERT: return Operation.CREATE;
            case RowLCR.DELETE: return Operation.DELETE;
            case RowLCR.UPDATE: return Operation.UPDATE;
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

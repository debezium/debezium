/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Objects;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * Base class for {@link ChangeRecordEmitter} implementations based on a relational database.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalChangeRecordEmitter implements ChangeRecordEmitter {

    private final OffsetContext offsetContext;
    private final Clock clock;

    public RelationalChangeRecordEmitter(OffsetContext offsetContext, Clock clock) {
        this.offsetContext = offsetContext;
        this.clock = clock;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        TableSchema tableSchema = (TableSchema) schema;
        Operation operation = getOperation();

        switch(operation) {
            case CREATE:
                emitCreateRecord(receiver, tableSchema, operation);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, tableSchema, operation);
                break;
            case DELETE:
                emitDeleteRecord(receiver, tableSchema, operation);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    private void emitCreateRecord(Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Object newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(newValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());

        receiver.changeRecord(operation, newKey, envelope, offsetContext);
    }

    private void emitUpdateRecord(Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

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

    private void emitDeleteRecord(Receiver receiver, TableSchema tableSchema, Operation operation)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, offsetContext.getSourceInfo(), clock.currentTimeInMillis());
        receiver.changeRecord(operation, oldKey, envelope, offsetContext);
    }

    protected abstract Operation getOperation();

    protected abstract Object[] getOldColumnValues();

    protected abstract Object[] getNewColumnValues();
}

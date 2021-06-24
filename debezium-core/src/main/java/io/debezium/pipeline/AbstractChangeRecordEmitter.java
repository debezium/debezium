/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * An abstract implementation of {@link io.debezium.pipeline.spi.ChangeRecordEmitter}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractChangeRecordEmitter<T extends DataCollectionSchema> implements ChangeRecordEmitter {

    private final OffsetContext offsetContext;
    private final Clock clock;

    public AbstractChangeRecordEmitter(OffsetContext offsetContext, Clock clock) {
        this.offsetContext = offsetContext;
        this.clock = clock;
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        Operation operation = getOperation();
        switch (operation) {
            case CREATE:
                emitCreateRecord(receiver, (T) schema);
                break;
            case READ:
                emitReadRecord(receiver, (T) schema);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, (T) schema);
                break;
            case DELETE:
                emitDeleteRecord(receiver, (T) schema);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    @Override
    public OffsetContext getOffset() {
        return offsetContext;
    }

    /**
     * Returns the clock of the change record(s) emitted.
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * Returns the operation associated with the change.
     */
    protected abstract Operation getOperation();

    /**
     * Emits change record(s) associated with a snapshot.
     *
     * @param receiver the handler for which the emitted record should be dispatched
     * @param schema the schema
     */
    protected abstract void emitReadRecord(Receiver receiver, T schema) throws InterruptedException;

    /**
     * Emits change record(s) associated with an insert operation.
     *
     * @param receiver the handler for which the emitted record should be dispatched
     * @param schema the schema
     */
    protected abstract void emitCreateRecord(Receiver receiver, T schema) throws InterruptedException;

    /**
     * Emits change record(s) associated with an update operation.
     *
     * @param receiver the handler for which the emitted record should be dispatched
     * @param schema the schema
     */
    protected abstract void emitUpdateRecord(Receiver receiver, T schema) throws InterruptedException;

    /**
     * Emits change record(s) associated with a delete operation.
     *
     * @param receiver the handler for which the emitted record should be dispatched
     * @param schema the schema
     */
    protected abstract void emitDeleteRecord(Receiver receiver, T schema) throws InterruptedException;
}

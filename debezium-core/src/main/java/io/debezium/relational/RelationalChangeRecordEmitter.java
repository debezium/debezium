/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Objects;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * Base class for {@link ChangeRecordEmitter} implementations based on a relational database.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalChangeRecordEmitter extends AbstractChangeRecordEmitter<TableSchema> {

    public static final String PK_UPDATE_OLDKEY_FIELD = "__debezium.oldkey";
    public static final String PK_UPDATE_NEWKEY_FIELD = "__debezium.newkey";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public RelationalChangeRecordEmitter(OffsetContext offsetContext, Clock clock) {
        super(offsetContext, clock);
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        TableSchema tableSchema = (TableSchema) schema;
        Operation operation = getOperation();

        switch (operation) {
            case CREATE:
                emitCreateRecord(receiver, tableSchema);
                break;
            case READ:
                emitReadRecord(receiver, tableSchema);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, tableSchema);
                break;
            case DELETE:
                emitDeleteRecord(receiver, tableSchema);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Object newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            logger.warn("no new values found for table '{}' from create message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }
        receiver.changeRecord(tableSchema, Operation.CREATE, newKey, envelope, getOffset(), null);
    }

    @Override
    protected void emitReadRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Object newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().read(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(tableSchema, Operation.READ, newKey, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Object oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Object newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            logger.warn("no new values found for table '{}' from update message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }
        // some configurations does not provide old values in case of updates
        // in this case we handle all updates as regular ones
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), null);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

            Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), headers);

            headers = new ConnectHeaders();
            headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

            envelope = tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(tableSchema, Operation.CREATE, newKey, envelope, getOffset(), headers);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
            logger.warn("no old values found for table '{}' from delete message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), null);
    }

    /**
     * Returns the operation done by the represented change.
     */
    protected abstract Operation getOperation();

    /**
     * Returns the old row state in case of an UPDATE or DELETE.
     */
    protected abstract Object[] getOldColumnValues();

    /**
     * Returns the new row state in case of a CREATE or READ.
     */
    protected abstract Object[] getNewColumnValues();

    /**
     * Whether empty data messages should be ignored.
     *
     * @return true if empty data messages coming from data source should be ignored.</br>
     * Typical use case are PostgreSQL changes without FULL replica identity.
     */
    protected boolean skipEmptyMessages() {
        return false;
    }
}

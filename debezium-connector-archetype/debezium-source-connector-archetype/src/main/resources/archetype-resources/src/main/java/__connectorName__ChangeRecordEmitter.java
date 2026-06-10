/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * Converts a raw source row into a Debezium change record and emits it to the framework.
 *
 * <p>The snapshot and streaming sources construct one instance per changed row and pass it to
 * {@link io.debezium.pipeline.EventDispatcher#dispatchDataChangeEvent}. The dispatcher then calls
 * {@link #emitChangeRecords}, supplying the registered {@link DataCollectionSchema} for the
 * collection. This class uses that schema together with the raw source data to build the key,
 * value, and envelope {@link Struct}s before forwarding them to the receiver.
 *
 * <p>Replace {@code Object} with your connector's native row or document type, and fill in each
 * {@code emitXxxRecord} method to produce the correct Structs for your data source.
 */
class ${connectorName}ChangeRecordEmitter
        extends AbstractChangeRecordEmitter<${connectorName}Partition, DataCollectionSchema> {

    private final Envelope.Operation operation;

    /**
     * Raw data from the source for this row. Replace {@code Object} with your connector's
     * native row type, for example a JDBC {@code ResultSet} row or a document map.
     */
    private final Object rowData;

    ${connectorName}ChangeRecordEmitter(${connectorName}Partition partition,
                                        ${connectorName}OffsetContext offsetContext,
                                        Envelope.Operation operation,
                                        Object rowData,
                                        Clock clock,
                                        ${connectorName}ConnectorConfig config) {
        super(partition, offsetContext, clock, config);
        this.operation = operation;
        this.rowData = rowData;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    protected void emitReadRecord(Receiver<${connectorName}Partition> receiver, DataCollectionSchema schema)
            throws InterruptedException {
        // Build the key Struct from rowData. schema.keySchema() gives the Connect key schema.
        // If you need a richer value schema, cast schema to your connector-specific subtype.
        Struct key = new Struct(schema.keySchema());
        // TODO: populate key fields, e.g. key.put("my_id", extractId(rowData));

        Struct value = null;
        // TODO: build value Struct from rowData using the collection's value schema.

        Struct envelope = schema.getEnvelopeSchema().read(value, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.READ, key, envelope, getOffset(), null);
    }

    @Override
    protected void emitCreateRecord(Receiver<${connectorName}Partition> receiver, DataCollectionSchema schema)
            throws InterruptedException {
        // TODO: same structure as emitReadRecord; use schema.getEnvelopeSchema().create(value, ...).
        throw new UnsupportedOperationException("TODO: implement CREATE emission");
    }

    @Override
    protected void emitUpdateRecord(Receiver<${connectorName}Partition> receiver, DataCollectionSchema schema)
            throws InterruptedException {
        // TODO: UPDATE may supply both old and new row state. Use schema.getEnvelopeSchema().update(before, after, ...).
        throw new UnsupportedOperationException("TODO: implement UPDATE emission");
    }

    @Override
    protected void emitDeleteRecord(Receiver<${connectorName}Partition> receiver, DataCollectionSchema schema)
            throws InterruptedException {
        // TODO: use schema.getEnvelopeSchema().delete(before, ...).
        throw new UnsupportedOperationException("TODO: implement DELETE emission");
    }
}

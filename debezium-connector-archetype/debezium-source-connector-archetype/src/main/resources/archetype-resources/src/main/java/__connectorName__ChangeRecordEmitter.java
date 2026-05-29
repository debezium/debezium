/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;

/**
 * Emits a pre-parsed row as a Debezium change record.
 *
 * <p>Holds the key and value {@link Struct}s built by the snapshot or streaming source
 * and hands them to the framework's {@link io.debezium.pipeline.EventDispatcher} via the
 * {@link ChangeRecordEmitter.Receiver} callback.
 */
class ${connectorName}ChangeRecordEmitter implements ChangeRecordEmitter<${connectorName}Partition> {

    private final ${connectorName}Partition partition;
    private final ${connectorName}OffsetContext offsetContext;
    private final Envelope.Operation operation;
    private final Struct keyStruct;
    private final Struct afterStruct;

    ${connectorName}ChangeRecordEmitter(${connectorName}Partition partition,
                                        ${connectorName}OffsetContext offsetContext,
                                        Envelope.Operation operation,
                                        Struct keyStruct,
                                        Struct afterStruct) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.operation = operation;
        this.keyStruct = keyStruct;
        this.afterStruct = afterStruct;
    }

    @Override
    public ${connectorName}Partition getPartition() {
        return partition;
    }

    @Override
    public OffsetContext getOffset() {
        return offsetContext;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver<${connectorName}Partition> receiver)
            throws InterruptedException {
        // Build the full envelope struct and pass it to the receiver.
        // The envelope schema comes from the collection schema registered for this data collection.
        var envelopeSchema = schema.getEnvelopeSchema();
        Struct envelope;
        switch (operation) {
            case READ, CREATE -> envelope = envelopeSchema.create(afterStruct, offsetContext.getSourceInfo(), null);
            case UPDATE -> envelope = envelopeSchema.update(null, afterStruct, offsetContext.getSourceInfo(), null);
            case DELETE -> envelope = envelopeSchema.delete(keyStruct, offsetContext.getSourceInfo(), null);
            default -> throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
        receiver.changeRecord(partition, schema, operation, keyStruct, envelope, offsetContext, null);
    }
}

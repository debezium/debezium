/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.bson.Document;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

import io.debezium.annotation.Immutable;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Clock;

/**
 * Emits change data based on a change stream change.
 *
 * @author Jiri Pechanec
 */
public class MongoDbChangeStreamChangeRecordEmitter extends AbstractChangeRecordEmitter<MongoDbCollectionSchema> {

    private final ChangeStreamDocument<Document> changeStreamEvent;

    @Immutable
    private static final Map<OperationType, Operation> OPERATION_LITERALS;

    static {
        Map<OperationType, Operation> literals = new HashMap<>();

        literals.put(OperationType.INSERT, Operation.CREATE);
        literals.put(OperationType.UPDATE, Operation.UPDATE);
        literals.put(OperationType.REPLACE, Operation.UPDATE);
        literals.put(OperationType.DELETE, Operation.DELETE);

        OPERATION_LITERALS = Collections.unmodifiableMap(literals);
    }

    public MongoDbChangeStreamChangeRecordEmitter(Partition partition, OffsetContext offsetContext, Clock clock, ChangeStreamDocument<Document> changeStreamEvent) {
        super(partition, offsetContext, clock);
        this.changeStreamEvent = changeStreamEvent;
    }

    @Override
    protected Operation getOperation() {
        return OPERATION_LITERALS.get(changeStreamEvent.getOperationType());
    }

    @Override
    protected void emitReadRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        // TODO Handled in MongoDbChangeSnapshotOplogRecordEmitter
        // It might be worthy haveing three classes - one for Snapshot, one for oplog and one for change streams
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    private void createAndEmitChangeRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        final Object newKey = schema.keyFromDocument(changeStreamEvent.getDocumentKey());
        assert newKey != null;

        final Struct value = schema.valueFromDocumentChangeStream(changeStreamEvent, getOperation());
        value.put(FieldName.SOURCE, getOffset().getSourceInfo());
        value.put(FieldName.OPERATION, getOperation().code());
        value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

        receiver.changeRecord(getPartition(), schema, getOperation(), newKey, value, getOffset(), null);
    }

    public static boolean isValidOperation(String operation) {
        return OPERATION_LITERALS.containsKey(operation);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.recordemitter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

import io.debezium.annotation.Immutable;
import io.debezium.connector.mongodb.MongoDbCollectionSchema;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Emits change data based on a change stream change.
 *
 * @author Jiri Pechanec
 */
public class MongoDbChangeRecordEmitter extends AbstractChangeRecordEmitter<MongoDbPartition, MongoDbCollectionSchema> {

    private final ChangeStreamDocument<BsonDocument> changeStreamEvent;

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

    public MongoDbChangeRecordEmitter(MongoDbPartition partition, OffsetContext offsetContext, Clock clock,
                                      ChangeStreamDocument<BsonDocument> changeStreamEvent) {
        super(partition, offsetContext, clock);
        this.changeStreamEvent = changeStreamEvent;
    }

    @Override
    public Operation getOperation() {
        return OPERATION_LITERALS.get(changeStreamEvent.getOperationType());
    }

    @Override
    protected void emitReadRecord(Receiver<MongoDbPartition> receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void emitCreateRecord(Receiver<MongoDbPartition> receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitUpdateRecord(Receiver<MongoDbPartition> receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitDeleteRecord(Receiver<MongoDbPartition> receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    private void createAndEmitChangeRecord(Receiver<MongoDbPartition> receiver, MongoDbCollectionSchema schema) throws InterruptedException {
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

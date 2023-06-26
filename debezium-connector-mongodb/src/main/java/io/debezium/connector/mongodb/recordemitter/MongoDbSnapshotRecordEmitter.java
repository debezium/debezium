/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.recordemitter;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;

import io.debezium.connector.mongodb.MongoDbCollectionSchema;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbPartition;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Emits change data based on a collection document.
 *
 * @author Chris Cranford
 */
public class MongoDbSnapshotRecordEmitter extends AbstractChangeRecordEmitter<MongoDbPartition, MongoDbCollectionSchema> {

    private final BsonDocument event;

    public MongoDbSnapshotRecordEmitter(MongoDbPartition partition, OffsetContext offsetContext, Clock clock, BsonDocument event,
                                        MongoDbConnectorConfig connectorConfig) {
        super(partition, offsetContext, clock, connectorConfig);
        this.event = event;
    }

    @Override
    public Operation getOperation() {
        return Operation.READ;
    }

    @Override
    protected void emitReadRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        final Object newKey = schema.keyFromDocumentSnapshot(event);
        assert newKey != null;

        final Struct value = schema.valueFromDocumentSnapshot(event, getOperation());
        value.put(FieldName.SOURCE, getOffset().getSourceInfo());
        value.put(FieldName.OPERATION, getOperation().code());
        value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

        receiver.changeRecord(getPartition(), schema, getOperation(), newKey, value, getOffset(), null);
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
}

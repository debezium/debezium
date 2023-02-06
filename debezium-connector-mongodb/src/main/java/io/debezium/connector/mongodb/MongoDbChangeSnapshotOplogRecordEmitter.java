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
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
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
public class MongoDbChangeSnapshotOplogRecordEmitter extends AbstractChangeRecordEmitter<MongoDbPartition, MongoDbCollectionSchema> {

    private final BsonDocument oplogEvent;

    /**
     * Whether this event originates from a snapshot.
     */
    private final boolean isSnapshot;
    /**
     * Whether raw_oplog is enabled.
     * Only MongoDbStreamingChangeEventSource might enable it.
     */
    private final boolean isRawOplogEnabled;

    @Immutable
    private static final Map<String, Operation> OPERATION_LITERALS;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbChangeSnapshotOplogRecordEmitter.class);

    static {
        Map<String, Operation> literals = new HashMap<>();

        literals.put("i", Operation.CREATE);
        literals.put("u", Operation.UPDATE);
        literals.put("d", Operation.DELETE);
        literals.put("n", Operation.NOOP);
        literals.put("c", Operation.COMMAND);

        OPERATION_LITERALS = Collections.unmodifiableMap(literals);
    }

    public MongoDbChangeSnapshotOplogRecordEmitter(MongoDbPartition partition, OffsetContext offsetContext, Clock clock, BsonDocument oplogEvent, boolean isSnapshot,
                                                   boolean isRawOplogEnabled) {
        super(partition, offsetContext, clock);
        this.oplogEvent = oplogEvent;
        this.isSnapshot = isSnapshot;
        this.isRawOplogEnabled = isRawOplogEnabled;
    }

    @Override
    public Operation getOperation() {
        if (isSnapshot || oplogEvent.getString("op").getValue() == null) {
            return Operation.READ;
        }
        return OPERATION_LITERALS.get(oplogEvent.getString("op").getValue());
    }

    @Override
    protected void emitReadRecord(Receiver receiver, MongoDbCollectionSchema schema) throws InterruptedException {
        final Object newKey = schema.keyFromDocumentOplog(oplogEvent);
        assert newKey != null;

        final Struct value = schema.valueFromDocumentOplog(oplogEvent, null, getOperation(), false);
        value.put(FieldName.SOURCE, getOffset().getSourceInfo());
        value.put(FieldName.OPERATION, getOperation().code());
        value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

        receiver.changeRecord(getPartition(), schema, getOperation(), newKey, value, getOffset(), null);
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
        BsonDocument patchObject = oplogEvent.getDocument("o");
        // Updates have an 'o2' field, since the updated object in 'o' might not have the ObjectID
        final BsonDocument filter = oplogEvent.containsKey("o2") ? oplogEvent.getDocument("o2") : oplogEvent.getDocument("o");

        final Object newKey = schema.keyFromDocumentOplog(filter);
        assert newKey != null;

        final Struct value = schema.valueFromDocumentOplog(patchObject, filter, getOperation(), isRawOplogEnabled);
        value.put(FieldName.SOURCE, getOffset().getSourceInfo());
        value.put(FieldName.OPERATION, getOperation().code());
        value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());
        if (isRawOplogEnabled) {
            if (!(oplogEvent instanceof RawBsonDocument)) {
                LOGGER.error("Expecting RawBsonDocument when RawOplogEnabled is true");
            }
            else {
                value.put(MongoDbFieldName.RAW_OPLOG_FIELD, documentToBytes((RawBsonDocument) oplogEvent));
            }
        }
        receiver.changeRecord(getPartition(), schema, getOperation(), newKey, value, getOffset(), null);
    }

    // This is thread-safe because it's we are creating new buffer and codec everytime
    private static byte[] documentToBytes(RawBsonDocument document) {
        ByteBuf buf = document.getByteBuffer();
        int originalPosition = buf.position();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        buf.position(originalPosition);
        return bytes;
    }

    public static boolean isValidOperation(String operation) {
        return OPERATION_LITERALS.containsKey(operation);
    }
}

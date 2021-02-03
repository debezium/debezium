/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;

/**
 * Emits change data.
 *
 * @author Jiri Pechanec
 */
public class MySqlChangeRecordEmitter implements ChangeRecordEmitter {

    private final Envelope.Operation operation;
    private final SourceRecord record;
    private final OffsetContext offset;

    public MySqlChangeRecordEmitter(OffsetContext offset, Envelope.Operation operation, SourceRecord record) {
        this.offset = offset;
        this.operation = operation;
        this.record = record;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        final Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }
        final String op = value.getString(Envelope.FieldName.OPERATION);
        receiver.changeRecord(schema, Envelope.Operation.forCode(op), record.key(), value, new OffsetContext() {

            @Override
            public Map<String, ?> getPartition() {
                return record.sourcePartition();
            }

            @Override
            public Map<String, ?> getOffset() {
                return record.sourceOffset();
            }

            @Override
            public Schema getSourceInfoSchema() {
                return value.getStruct("source").schema();
            }

            @Override
            public Struct getSourceInfo() {
                return value.getStruct("source");
            }

            @Override
            public boolean isSnapshotRunning() {
                return false;
            }

            @Override
            public void markLastSnapshotRecord() {
            }

            @Override
            public void preSnapshotStart() {
            }

            @Override
            public void preSnapshotCompletion() {
            }

            @Override
            public void postSnapshotCompletion() {
            }

            @Override
            public void event(DataCollectionId collectionId, Instant timestamp) {
            }

            @Override
            public TransactionContext getTransactionContext() {
                // TODO Remove with RecordMakers rewrite
                return null;
            }
        }, (ConnectHeaders) record.headers());
    }

    @Override
    public OffsetContext getOffset() {
        return offset;
    }
}

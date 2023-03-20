/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.Serializable;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data.
 *
 * @author Jiri Pechanec
 */
public class MySqlChangeRecordEmitter extends RelationalChangeRecordEmitter<MySqlPartition> {

    private final Envelope.Operation operation;
    private final OffsetContext offset;
    private final Object[] before;
    private final Object[] after;
    private final boolean skipMessagesWithoutChange;

    public MySqlChangeRecordEmitter(MySqlPartition partition, OffsetContext offset, Clock clock, Operation operation, Serializable[] before,
                                    Serializable[] after, boolean skipMessagesWithoutChange) {
        super(partition, offset, clock);
        this.offset = offset;
        this.operation = operation;
        this.before = before;
        this.after = after;
        this.skipMessagesWithoutChange = skipMessagesWithoutChange;
    }

    @Override
    public OffsetContext getOffset() {
        return offset;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
    }

    @Override
    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    @Override
    public boolean skipMessagesWithoutChange() {
        return skipMessagesWithoutChange;
    }
}

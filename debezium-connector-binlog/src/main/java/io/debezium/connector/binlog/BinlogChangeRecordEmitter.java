/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.io.Serializable;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data.
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
public class BinlogChangeRecordEmitter<P extends BinlogPartition> extends RelationalChangeRecordEmitter<P> {

    private final Operation operation;
    private final OffsetContext offset;
    private final Object[] before;
    private final Object[] after;

    public BinlogChangeRecordEmitter(P partition, OffsetContext offset, Clock clock, Operation operation,
                                     Serializable[] before, Serializable[] after, BinlogConnectorConfig connectorConfig) {
        super(partition, offset, clock, connectorConfig);
        this.offset = offset;
        this.operation = operation;
        this.before = before;
        this.after = after;
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
    protected void emitTruncateRecord(Receiver<P> receiver, TableSchema schema) throws InterruptedException {
        Struct envelope = schema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.Serializable;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Emits change data.
 *
 * @author Jiri Pechanec
 */
public class MySqlChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final Envelope.Operation operation;
    private final OffsetContext offset;
    private final Object[] before;
    private final Object[] after;

    public MySqlChangeRecordEmitter(OffsetContext offset, Clock clock, Envelope.Operation operation, Serializable[] before, Serializable[] after) {
        super(offset, clock);
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
    protected Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before != null ? before : null;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after != null ? after : null;
    }
}

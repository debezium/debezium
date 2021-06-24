/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single row read via JDBC.
 *
 * @author Jiri Pechanec
 */
public class SnapshotChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final Object[] row;

    public SnapshotChangeRecordEmitter(OffsetContext offset, Object[] row, Clock clock) {
        super(offset, clock);

        this.row = row;
    }

    @Override
    protected Operation getOperation() {
        return Operation.READ;
    }

    @Override
    protected Object[] getOldColumnValues() {
        throw new UnsupportedOperationException("Can't get old row values for READ record");
    }

    @Override
    protected Object[] getNewColumnValues() {
        return row;
    }
}

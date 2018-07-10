/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;
import oracle.streams.RowLCR;

/**
 * Emits change data based on a single {@link RowLCR} event.
 *
 * @author Gunnar Morling
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

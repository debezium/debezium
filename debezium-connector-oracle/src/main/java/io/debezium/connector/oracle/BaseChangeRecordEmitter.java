/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Base class to emit change data based on a single entry event.
 */
public abstract class BaseChangeRecordEmitter<T> extends RelationalChangeRecordEmitter {

    protected final Table table;

    protected BaseChangeRecordEmitter(Partition partition, OffsetContext offset, Table table, Clock clock) {
        super(partition, offset, clock);
        this.table = table;
    }

    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

}

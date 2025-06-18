/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.RecordMakers.Operation;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Emits change records from parsed CockroachDB changefeed messages using Debezium's SPI.
 * This class encapsulates a single changefeed message and emits its key/value pair,
 * operation type, and source metadata.
 *
 * @author Virag Tripathi
 */
public class CockroachDBChangeRecordEmitter implements ChangeRecordEmitter {

    private final OffsetContext offsetContext;
    private final Instant timestamp;
    private final Object key;
    private final Struct value;
    private final Operation operation;

    public CockroachDBChangeRecordEmitter(OffsetContext offsetContext,
                                          Instant timestamp,
                                          Object key,
                                          Struct value,
                                          Operation operation) {
        this.offsetContext = offsetContext;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.operation = operation;
    }

    @Override
    public OffsetContext getOffset() {
        return offsetContext;
    }

    @Override
    public <T extends DataCollectionId> void emitChangeRecords(DataCollectionSchema schema,
                                                               Receiver<T> receiver)
            throws InterruptedException {
        receiver.changeRecord(
                null, // partition, not used here
                schema,
                operation.getEnvelopeOperation(),
                key,
                value,
                offsetContext,
                null);
    }
}

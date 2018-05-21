/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.schema.DataCollectionSchema;

/**
 * Emits one or more change records - specific to a given {@link DataCollectionSchema}.
 *
 * @author Gunnar Morling
 */
public interface ChangeRecordEmitter {

    void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException;

    public interface Receiver {
        void changeRecord(Operation operation, Object key, Struct value, OffsetContext offsetManager) throws InterruptedException;
    }
}

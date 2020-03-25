/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import io.debezium.data.Envelope.Operation;
import io.debezium.schema.DataCollectionSchema;

/**
 * Represents a change applied to a source database and emits one or more corresponding change records. In most cases,
 * there'll be one change record for one source change, but e.g. in case of updates involving a record's PK, it may
 * result in a deletion and re-insertion record.
 *
 * @author Gunnar Morling
 */
public interface ChangeRecordEmitter {

    /**
     * Emits the change record(s) corresponding to data change represented by this emitter.
     */
    void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException;

    /**
     * Returns the offset of the change record(s) emitted.
     */
    OffsetContext getOffset();

    /**
     * Callback passed to {@link ChangeRecordEmitter}s, allowing them to produce one
     * or more change records.
     */
    public interface Receiver {

        void changeRecord(DataCollectionSchema schema, Operation operation, Object key, Struct value,
                          OffsetContext offset, ConnectHeaders headers)
                throws InterruptedException;
    }
}

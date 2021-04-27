/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.util.ApproximateStructSizeCalculator;

/**
 * A data change event wrapping a {@link SourceRecord}.
 *
 * @author Gunnar Morling
 */
public class SourceRecordChangeEvent implements DataChangeEvent {

    private final SourceRecord record;
    private final long sizeInBytes;

    public SourceRecordChangeEvent(SourceRecord record) {
        this.record = record;
        this.sizeInBytes = ApproximateStructSizeCalculator.getApproximateRecordSize(record) + 32;
    }

    public SourceRecord getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "DataChangeEvent [record=" + record + "]";
    }

    @Override
    public long getApproximateSizeInBytes() {
        return sizeInBytes;
    }
}

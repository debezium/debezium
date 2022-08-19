/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.util.ApproximateStructSizeCalculator;

public class DataChangeEvent implements Sizeable {

    private final SourceRecord record;

    public DataChangeEvent(SourceRecord record) {
        this.record = record;
    }

    public SourceRecord getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "DataChangeEvent [record=" + record + "]";
    }

    @Override
    public long objectSize() {
        return ApproximateStructSizeCalculator.getApproximateRecordSize(record);
    }
}

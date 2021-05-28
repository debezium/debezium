/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import io.debezium.connector.common.SourceRecordWrapper;

public class DataChangeEvent {

    private final SourceRecordWrapper record;

    public DataChangeEvent(SourceRecordWrapper record) {
        this.record = record;
    }

    public SourceRecordWrapper getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "DataChangeEvent [record=" + record + "]";
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.avro.Schema;

/**
 * An internal representation of a create/update/delete event.
 */
public class ChangeRecord extends Record {

    public ChangeRecord(SourceInfo source, RowData rowData, Schema keySchema, Schema valueSchema, Operation op, boolean markOffset) {
        super(source, rowData, keySchema, valueSchema, op, markOffset, System.currentTimeMillis());
    }

    @Override
    public EventType getEventType() {
        return EventType.CHANGE_EVENT;
    }
}

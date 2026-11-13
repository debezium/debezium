/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

public class FlatSinkRecordFactory implements SinkRecordFactory {

    @Override
    public boolean isFlattened() {
        return true;
    }

    @Override
    public SinkRecord buildRecord(String topic, Schema recordSchema, Struct after, Schema keySchema, Struct key, long offset,
                                  Envelope.Operation op, Headers headers) {
        return new SinkRecord(topic, 0, keySchema, key, recordSchema, after, offset, null, null, headers);
    }

    @Override
    public SinkRecord buildDeleteRecord(String topic, Schema recordSchema, Struct before, Schema keySchema, Struct key, long offset, Headers headers) {
        return new SinkRecord(topic, 0, keySchema, key, recordSchema, null, offset, null, null, headers);
    }
}

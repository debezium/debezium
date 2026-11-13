/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

public class EnvelopeSinkRecordFactory implements SinkRecordFactory {

    @Override
    public boolean isFlattened() {
        return false;
    }

    @Override
    public SinkRecord buildRecord(String topic, Schema recordSchema, Struct after, Schema keySchema, Struct key, long offset,
                                  Envelope.Operation op, Headers headers) {
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, op.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        return new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, offset, null, null, headers);
    }

    @Override
    public SinkRecord buildDeleteRecord(String topic, Schema recordSchema, Struct before, Schema keySchema, Struct key, long offset, Headers headers) {
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.BEFORE, before)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.DELETE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        return new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, offset, null, null, headers);
    }
}

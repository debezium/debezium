/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.junit;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

public class SinkRecordBuilder {

    private final SinkRecordFactory factory;
    private final String topic;
    private final Map<String, FieldEntry> fields = new LinkedHashMap<>();
    private final Map<String, FieldEntry> keyFields = new LinkedHashMap<>();
    private final ConnectHeaders headers = new ConnectHeaders();
    private Envelope.Operation operation = Envelope.Operation.CREATE;

    SinkRecordBuilder(SinkRecordFactory factory, String topic) {
        this.factory = factory;
        this.topic = topic;
    }

    public SinkRecordBuilder field(String name, Schema schema, Object value) {
        fields.put(name, new FieldEntry(schema, value));
        return this;
    }

    public SinkRecordBuilder keyField(String name, Schema schema, Object value) {
        keyFields.put(name, new FieldEntry(schema, value));
        return this;
    }

    public SinkRecordBuilder without(String fieldName) {
        fields.remove(fieldName);
        return this;
    }

    public SinkRecordBuilder header(String key, Schema schema, Object value) {
        headers.add(key, value, schema);
        return this;
    }

    public SinkRecordBuilder operation(Envelope.Operation op) {
        this.operation = op;
        return this;
    }

    public SinkRecord build(long offset) {
        Schema recordSchema = buildSchema(fields);
        Struct recordStruct = buildStruct(recordSchema, fields);

        Schema keySchema = buildSchema(keyFields);
        Struct keyStruct = keyFields.isEmpty() ? null : buildStruct(keySchema, keyFields);

        if (operation == Envelope.Operation.DELETE) {
            return factory.buildDeleteRecord(topic, recordSchema, recordStruct, keySchema, keyStruct, offset, headers);
        }
        return factory.buildRecord(topic, recordSchema, recordStruct, keySchema, keyStruct, offset, operation, headers);
    }

    private Schema buildSchema(Map<String, FieldEntry> entries) {
        SchemaBuilder builder = SchemaBuilder.struct();
        entries.forEach((name, entry) -> builder.field(name, entry.schema));
        return builder.build();
    }

    private Struct buildStruct(Schema schema, Map<String, FieldEntry> entries) {
        Struct struct = new Struct(schema);
        entries.forEach((name, entry) -> struct.put(name, entry.value));
        return struct;
    }

    private record FieldEntry(Schema schema, Object value) {
    }
}

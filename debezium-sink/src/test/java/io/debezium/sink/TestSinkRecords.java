/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.data.Envelope;

public final class TestSinkRecords {

    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    private TestSinkRecords() {
    }

    public static KafkaDebeziumSinkRecord flat(String topic, byte id, String name) {
        Schema valueSchema = flatSchema();
        Struct value = new Struct(valueSchema).put("id", id).put("name", name);
        SinkRecord sr = new SinkRecord(topic, 0, null, null, valueSchema, value, 0);
        return new KafkaDebeziumSinkRecord(sr, CE_PATTERN);
    }

    public static KafkaDebeziumSinkRecord keyed(String topic, Object key) {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema).put("id", key.toString());
        Schema valueSchema = flatSchema();
        Struct value = new Struct(valueSchema).put("id", (byte) 1).put("name", "test");
        SinkRecord sr = new SinkRecord(topic, 0, keySchema, keyStruct, valueSchema, value, 0);
        return new KafkaDebeziumSinkRecord(sr, CE_PATTERN);
    }

    public static KafkaDebeziumSinkRecord truncate(String topic) {
        Schema sourceSchema = SchemaBuilder.struct()
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.SOURCE, new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli()));
        envelope.put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code());
        envelope.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        SinkRecord sr = new SinkRecord(topic, 0, null, null, envelopeSchema, envelope, 0);
        return new KafkaDebeziumSinkRecord(sr, CE_PATTERN);
    }

    public static KafkaDebeziumSinkRecord debeziumCreate(String topic, byte id, String name, String sourceDb, String sourceTable) {
        Schema recordSchema = flatSchema();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.BEFORE, recordSchema)
                .field(Envelope.FieldName.AFTER, recordSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct envelope = new Struct(envelopeSchema);
        envelope.put(Envelope.FieldName.AFTER, new Struct(recordSchema).put("id", id).put("name", name));
        envelope.put(Envelope.FieldName.SOURCE, new Struct(sourceSchema)
                .put("ts_ms", Instant.now().toEpochMilli())
                .put("db", sourceDb)
                .put("table", sourceTable));
        envelope.put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code());
        envelope.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT8_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);

        SinkRecord sr = new SinkRecord(topic, 0, keySchema, key, envelopeSchema, envelope, 0);
        return new KafkaDebeziumSinkRecord(sr, CE_PATTERN);
    }

    private static Schema flatSchema() {
        return SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }
}

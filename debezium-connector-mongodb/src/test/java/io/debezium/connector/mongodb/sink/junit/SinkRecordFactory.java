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
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

public interface SinkRecordFactory {

    Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    boolean isFlattened();

    default SinkRecordBuilder record(String topic) {
        return new SinkRecordBuilder(this, topic);
    }

    default SinkRecordBuilder createProduct(String topicPrefix, int id, String name, String description, double weight) {
        return record(topicPrefix + ".inventory.products")
                .keyField("id", Schema.INT32_SCHEMA, id)
                .field("id", Schema.INT32_SCHEMA, id)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, name)
                .field("description", Schema.OPTIONAL_STRING_SCHEMA, description)
                .field("weight", Schema.OPTIONAL_FLOAT64_SCHEMA, weight);
    }

    default SinkRecordBuilder createCustomer(String topicPrefix, int id, String firstName, String lastName, String email) {
        return record(topicPrefix + ".inventory.customers")
                .keyField("id", Schema.INT32_SCHEMA, id)
                .field("id", Schema.INT32_SCHEMA, id)
                .field("first_name", Schema.OPTIONAL_STRING_SCHEMA, firstName)
                .field("last_name", Schema.OPTIONAL_STRING_SCHEMA, lastName)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA, email);
    }

    default SinkRecordBuilder updateProduct(String topicPrefix, int id, String name, String description, double weight) {
        return createProduct(topicPrefix, id, name, description, weight)
                .operation(Envelope.Operation.UPDATE);
    }

    default SinkRecordBuilder deleteProduct(String topicPrefix, int id) {
        return record(topicPrefix + ".inventory.products")
                .keyField("id", Schema.INT32_SCHEMA, id)
                .field("id", Schema.INT32_SCHEMA, id)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA, "deleted")
                .field("description", Schema.OPTIONAL_STRING_SCHEMA, "gone")
                .field("weight", Schema.OPTIONAL_FLOAT64_SCHEMA, 0.0)
                .operation(Envelope.Operation.DELETE);
    }

    default SinkRecord tombstone(String topicPrefix, int id, long offset) {
        String topic = topicPrefix + ".inventory.products";
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", id);
        return new SinkRecord(topic, 0, keySchema, key, null, null, offset);
    }

    default SinkRecord truncateProduct(String topicPrefix, long offset) {
        String topic = topicPrefix + ".inventory.products";
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topic + ".Envelope")
                .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", Instant.now().toEpochMilli());
        Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());

        return new SinkRecord(topic, 0, null, null, envelopeSchema, envelope, offset);
    }

    SinkRecord buildRecord(String topic, Schema recordSchema, Struct after, Schema keySchema, Struct key, long offset,
                           Envelope.Operation op, org.apache.kafka.connect.header.Headers headers);

    SinkRecord buildDeleteRecord(String topic, Schema recordSchema, Struct before, Schema keySchema, Struct key, long offset,
                                 org.apache.kafka.connect.header.Headers headers);

    static Struct buildKey(int keyId) {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        return new Struct(keySchema).put("id", keyId);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

/**
 * Unit test for {@link MongoToRelationalConverter}.
 *
 * @author Divyansh Agrawal
 */
public class MongoToRelationalConverterTest {

    private MongoToRelationalConverter<SourceRecord> transformation;

    @BeforeEach
    void setup() {
        transformation = new MongoToRelationalConverter<>();
        transformation.configure(new java.util.HashMap<>());
    }

    @AfterEach
    void closeSmt() {
        transformation.close();
    }

    @Test
    public void shouldPassHeartbeatMessages() {
        // Heartbeat messages sent by Debezium don't have standard MongoDB envelope shapes.
        // The SMT must be smart enough to detect this and seamlessly pass them through without crashing.

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.Heartbeat")
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema).put(AbstractSourceInfo.TIMESTAMP_KEY, 1565787098802L);

        Schema keySchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.ServerNameKey")
                .field("serverName", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("serverName", "op.with.heartbeat");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "op.with.heartbeat",
                keySchema,
                key,
                valueSchema,
                value);

        // Act: Apply the Transformation
        SourceRecord transformed = transformation.apply(eventRecord);

        // Assert: It should return the exact same, completely untouched record reference
        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    public void shouldConvertBsonStringsToStructs() {
        // GIVEN: A raw, unmodified Debezium MongoDB SourceRecord

        // 1. Build a mock "Envelope" schema. Notice how 'before' and 'after' are merely OPTIONAL_STRING_SCHEMA.
        Schema sourceSchema = SchemaBuilder.struct().name("io.debezium.connector.mongo.Source").build();
        Schema recordSchema = SchemaBuilder.struct().name("server.db.collection.Envelope")
                .field(Envelope.FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                .build();

        // 2. Mock the actual event. The documents are stringified JSON format emitted by the MongoDB connector.
        // Note: The 'after' state contains a new "age" field not present in the 'before' state.
        Struct recordValue = new Struct(recordSchema);
        recordValue.put(Envelope.FieldName.BEFORE, "{\"_id\": 1, \"name\": \"old_name\"}");
        recordValue.put(Envelope.FieldName.AFTER, "{\"_id\": 1, \"name\": \"new_name\", \"age\": 30}");
        recordValue.put(Envelope.FieldName.SOURCE, new Struct(sourceSchema));
        recordValue.put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
        recordValue.put(Envelope.FieldName.TIMESTAMP, 123456789L);

        SourceRecord record = new SourceRecord(
                new HashMap<>(), new HashMap<>(),
                "server.db.collection",
                null, null,
                recordSchema, recordValue);

        // When: The SMT processes the MongoDB record
        SourceRecord transformed = transformation.apply(record);

        // Then: The output should be a normalized, relational-style record

        Struct transformedValue = (Struct) transformed.value();
        Schema transformedSchema = transformed.valueSchema();

        // 1. Standard metadata (timestamps, operation types) should be perfectly preserved
        assertThat(transformedValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.UPDATE.code());
        assertThat(transformedValue.getInt64(Envelope.FieldName.TIMESTAMP)).isEqualTo(123456789L);

        // 2. The 'after' field should now be a real Struct with strong typing, NOT a JSON string
        Struct afterStruct = transformedValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(afterStruct).isNotNull();
        assertThat(afterStruct.getInt32("_id")).isEqualTo(1);
        assertThat(afterStruct.getString("name")).isEqualTo("new_name");
        assertThat(afterStruct.getInt32("age")).isEqualTo(30);

        // 3. The 'before' field should also be a Struct, sharing the EXACT SAME schema as 'after'
        Struct beforeStruct = transformedValue.getStruct(Envelope.FieldName.BEFORE);
        assertThat(beforeStruct).isNotNull();
        assertThat(beforeStruct.getInt32("_id")).isEqualTo(1);
        assertThat(beforeStruct.getString("name")).isEqualTo("old_name");

        // 4. Missing Field Injection Check:
        // Because the unified schema inferred that 'age' exists (from the 'after' doc),
        // the SMT must inject a null value for 'age' in the 'before' doc rather than crashing.
        assertThat(beforeStruct.get("age")).isNull();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;

public class TransformsUtils {

    public static final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string())
            .build();

    public static final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", SchemaBuilder.int32())
            .build();

    public static final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    public static SourceRecord createDeleteRecord(int id) {
        final Schema deleteSourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .build();

        Envelope deleteEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(deleteSourceSchema)
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(deleteSourceSchema);

        before.put("id", (byte) id);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("idh", id);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy" + id, 0,
                null, null,
                envelope.schema(), payload,
                (long) id,
                headers);
    }

    public static SourceRecord createDeleteCustomerRecord(int id) {
        final Schema deleteSourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .build();

        Envelope deleteEnvelope = Envelope.defineSchema()
                .withName("customer.Envelope")
                .withRecord(recordSchema)
                .withSource(deleteSourceSchema)
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(deleteSourceSchema);

        before.put("id", (byte) id);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "customer", envelope.schema(), payload);
    }

    public static SourceRecord createMongoDbRecord() {
        final Schema insertSourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .build();

        final Envelope insertEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(Schema.STRING_SCHEMA)
                .withSource(insertSourceSchema)
                .build();

        final Struct source = new Struct(insertSourceSchema);

        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = insertEnvelope.create(
                "{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}", source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "original", envelope.schema(), payload);
    }

    public static SourceRecord createNullRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null, null, null);
    }

    public static SourceRecord createComplexCreateRecord() {
        final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.int8()).build();
        final Schema sourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    public static SourceRecord createAllDataTypeRecord(boolean boolValue) {
        final Schema sourceSchema = SchemaBuilder.struct()
                .field("0", SchemaBuilder.BOOLEAN_SCHEMA).required()
                .field("1", SchemaBuilder.BYTES_SCHEMA).required()
                .field("2", SchemaBuilder.FLOAT32_SCHEMA).required()
                .field("3", SchemaBuilder.FLOAT64_SCHEMA).required()
                .field("4", SchemaBuilder.INT16_SCHEMA).required()
                .field("5", SchemaBuilder.INT32_SCHEMA).required()
                .field("6", SchemaBuilder.INT64_SCHEMA).required()
                .field("7", SchemaBuilder.INT8_SCHEMA).required()
                .field("8", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
                .field("9", SchemaBuilder.OPTIONAL_BYTES_SCHEMA)
                .field("10", SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA)
                .field("11", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                .field("12", SchemaBuilder.OPTIONAL_INT16_SCHEMA)
                .field("13", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                .field("14", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                .field("15", SchemaBuilder.OPTIONAL_INT8_SCHEMA)
                .field("16", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .field("17", SchemaBuilder.STRING_SCHEMA).required()
                .build();
        final Schema recordSchema = SchemaBuilder.struct().build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        final Struct source = new Struct(sourceSchema);
        final Struct before = new Struct(recordSchema);

        source.put("0", boolValue);
        source.put("1", new byte[]{ 1 });
        source.put("2", 2.2f);
        source.put("3", 3.3d);
        source.put("4", (short) 4);
        source.put("5", 5);
        source.put("6", 6L);
        source.put("7", (byte) 7);
        source.put("17", "seventeen");
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    public static SourceRecord createArrayDataTypeRecord(boolean boolValue) {
        final Schema sourceElementSchema = SchemaBuilder.struct()
                .field("bool", SchemaBuilder.BOOLEAN_SCHEMA).required()
                .field("int", SchemaBuilder.INT32_SCHEMA).required()
                .build();
        final Schema sourceSchema = SchemaBuilder.struct()
                .field("arr", SchemaBuilder.array(sourceElementSchema).build())
                .build();
        final Schema recordSchema = SchemaBuilder.struct().build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        final Struct source = new Struct(sourceSchema);
        final Struct before = new Struct(recordSchema);

        var elem0 = new Struct(sourceElementSchema);
        elem0.put("bool", boolValue);
        elem0.put("int", 123);
        var elem1 = new Struct(sourceElementSchema);
        elem1.put("bool", false);
        elem1.put("int", 321);
        source.put("arr", List.of(elem0, elem1));
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

}

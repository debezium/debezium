/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.time.Instant;
import java.util.HashMap;

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

}

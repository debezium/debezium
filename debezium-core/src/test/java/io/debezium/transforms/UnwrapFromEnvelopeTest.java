/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

/**
 * @author Jiri Pechanec
 */
public class UnwrapFromEnvelopeTest {

    private static final String DROP_DELETES = "drop.deletes";
    private static final String DROP_TOMBSTONES = "drop.tombstones";

    @Test
    public void testTombstoneDroppedByDefault() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assert transform.apply(tombstone) == null;
        }
    }

    @Test
    public void testTombstoneDroppedConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "true");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assert transform.apply(tombstone) == null;
        }
    }

    @Test
    public void testTombstoneForwardConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "false");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assert transform.apply(tombstone) == tombstone;
        }
    }

    private SourceRecord createDeleteRecord() {
        final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.int8()).build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte)1);
        final Struct payload = envelope.delete(before, null, System.nanoTime());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createCreateRecord() {
        final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.int8()).build();
        Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(SchemaBuilder.struct().build())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte)1);
        final Struct payload = envelope.create(before, null, System.nanoTime());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createUnknownRecord() {
        final Schema recordSchema = SchemaBuilder.struct().name("unknown")
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte)1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    private SourceRecord createUnknownUnnamedSchemaRecord() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte)1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    @Test
    public void testDeleteDroppedByDefault() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assert transform.apply(deleteRecord) == null;
        }
    }

    @Test
    public void testDeleteDroppedConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_DELETES, "true");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assert transform.apply(deleteRecord) == null;
        }
    }

    @Test
    public void testDeleteFrowardConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_DELETES, "false");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord tombstone = transform.apply(deleteRecord);
            assert tombstone.value() == null;
        }
    }

    @Test
    public void testUnwrapCreateRecord() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assert ((Struct)unwrapped.value()).getInt8("id") == 1;
        }
    }

    @Test
    public void testIgnoreUnknownRecord() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assert transform.apply(unknownRecord) == unknownRecord;

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assert transform.apply(unnamedSchemaRecord) == unnamedSchemaRecord;
        }
    }
}

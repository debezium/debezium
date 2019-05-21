/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * @author Jiri Pechanec
 */
public class UnwrapFromEnvelopeTest {

    private static final String DROP_TOMBSTONES = "drop.tombstones";
    private static final String HANDLE_DELETES = "delete.handling.mode";
    private static final String OPERATION_HEADER = "operation.header";

    @Test
    public void testTombstoneDroppedByDefault() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneDroppedConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "true");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneForwardConfigured() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "false");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isEqualTo(tombstone);
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
        before.put("id", (byte) 1);
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
        before.put("id", (byte) 1);
        final Struct payload = envelope.create(before, null, System.nanoTime());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createUnknownRecord() {
        final Schema recordSchema = SchemaBuilder.struct().name("unknown")
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    private SourceRecord createUnknownUnnamedSchemaRecord() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    private String getSourceRecordHeaderByKey(SourceRecord record, String headerKey) {
        Iterator<Header> operationHeader = record.headers().allWithName(headerKey);
        if (!operationHeader.hasNext()) {
            return null;
        }

        return operationHeader.next().value().toString();
    }

    @Test
    public void testDeleteDroppedByDefault() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteDrop() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteNone() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "none");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord tombstone = transform.apply(deleteRecord);
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test
    public void testHandleDeleteRewrite() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
        }
    }

    @Test
    public void testHandleCreateRewrite() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(OPERATION_HEADER, "true");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("false");
            assertThat(unwrapped.headers()).hasSize(1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, UnwrapFromEnvelope.DEBEZIUM_OPERATION_HEADER_KEY);
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    public void testUnwrapCreateRecord() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testIgnoreUnknownRecord() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
        }
    }

    @Test
    @FixFor("DBZ-971")
    public void testUnwrapPropagatesRecordHeaders() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            createRecord.headers().addString("application/debezium-test-header", "shouldPropagatePreviousRecordHeaders");

            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);

            assertThat(unwrapped.headers()).hasSize(1);
            Iterator<Header> headers = unwrapped.headers().allWithName("application/debezium-test-header");
            assertThat(headers.hasNext()).isTrue();
            assertThat(headers.next().value().toString()).isEqualTo("shouldPropagatePreviousRecordHeaders");
        }
    }
}

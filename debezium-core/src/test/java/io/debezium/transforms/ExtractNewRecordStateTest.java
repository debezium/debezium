/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
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
import io.debezium.pipeline.txmetadata.TransactionMonitor;

/**
 * @author Jiri Pechanec
 */
public class ExtractNewRecordStateTest {

    private static final String DROP_TOMBSTONES = "drop.tombstones";
    private static final String HANDLE_DELETES = "delete.handling.mode";
    private static final String ROUTE_BY_FIELD = "route.by.field";
    private static final String ADD_FIELDS = "add.fields";
    private static final String ADD_HEADERS = "add.headers";
    private static final String ADD_FIELDS_PREFIX = ADD_FIELDS + ".prefix";
    private static final String ADD_HEADERS_PREFIX = ADD_HEADERS + ".prefix";

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string())
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", SchemaBuilder.int32())
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    @Test
    public void testTombstoneDroppedByDefault() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneDroppedConfigured() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "true");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneForwardConfigured() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(DROP_TOMBSTONES, "false");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isEqualTo(tombstone);
        }
    }

    private SourceRecord createDeleteRecord() {
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

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createTombstoneRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
    }

    private SourceRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createUpdateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createComplexCreateRecord() {
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

        Object value = operationHeader.next().value();

        return value != null ? value.toString() : null;
    }

    @Test
    public void testDeleteDroppedByDefault() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteDrop() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteNone() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
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
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
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
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("false");
            assertThat(unwrapped.headers()).hasSize(1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    public void testUnwrapCreateRecord() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testIgnoreUnknownRecord() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
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
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
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

    @Test
    @FixFor("DBZ-1452")
    public void testAddField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    @FixFor({ "DBZ-1452", "DBZ-2504" })
    public void testAddFields() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op , lsn,id");
            props.put(ADD_FIELDS_PREFIX, "prefix.");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get("prefix.op")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get("prefix.lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("prefix.id")).isEqualTo("571");
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsForMissingOptionalField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op,lsn,id");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("__id")).isEqualTo(null);
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsSpecifyStruct() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op,source.lsn,transaction.id,transaction.total_order");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get("__source_lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("__transaction_id")).isEqualTo("571");
            assertThat(((Struct) unwrapped.value()).get("__transaction_total_order")).isEqualTo(42L);
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeader() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeaders() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op , lsn,id");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.headers()).hasSize(3);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.UPDATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__id");
            assertThat(headerValue).isEqualTo(String.valueOf(571L));
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeadersForMissingOptionalField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op,lsn,id");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(3);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__id");
            assertThat(headerValue).isNull();
        }
    }

    @Test
    @FixFor({ "DBZ-1452", "DBZ-2504" })
    public void testAddHeadersSpecifyStruct() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op,source.lsn,transaction.id,transaction.total_order");
            props.put(ADD_HEADERS_PREFIX, "prefix.");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.headers()).hasSize(4);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.UPDATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.source_lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.transaction_id");
            assertThat(headerValue).isEqualTo(String.valueOf(571L));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.transaction_total_order");
            assertThat(headerValue).isEqualTo(String.valueOf(42L));
        }
    }

    @Test
    public void testAddTopicRoutingField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ROUTE_BY_FIELD, "name");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrappedCreate = transform.apply(createRecord);
            assertThat(unwrappedCreate.topic()).isEqualTo("myRecord");
        }
    }

    @Test
    public void testUpdateTopicRoutingField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ROUTE_BY_FIELD, "name");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.topic()).isEqualTo("updatedRecord");
        }
    }

    @Test
    public void testDeleteTopicRoutingField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ROUTE_BY_FIELD, "name");
            props.put(HANDLE_DELETES, "none");

            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord).topic()).isEqualTo("myRecord");
        }
    }

    @Test
    @FixFor("DBZ-1876")
    public void testAddHeadersHandleDeleteRewriteAndTombstone() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_HEADERS, "op,source.lsn");
            props.put(DROP_TOMBSTONES, "false");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.DELETE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__source_lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));

            final SourceRecord tombstone = transform.apply(createTombstoneRecord());
            assertThat(getSourceRecordHeaderByKey(tombstone, "__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddFieldNonExistantField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "nope");
            transform.configure(props);

            final SourceRecord createRecord = createComplexCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);

            assertThat(((Struct) unwrapped.value()).schema().field("__nope")).isNull();
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldHandleDeleteRewrite() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsHandleDeleteRewrite() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op,lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1876")
    public void testAddFieldsHandleDeleteRewriteAndTombstone() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op,lsn");
            props.put(DROP_TOMBSTONES, "false");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);

            final SourceRecord tombstone = transform.apply(createTombstoneRecord());
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsSpecifyStructHandleDeleteRewrite() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op,source.lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__source_lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1517")
    public void testSchemaChangeEventWithOperationHeader() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
        }
    }
}

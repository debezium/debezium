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

import org.apache.kafka.common.config.ConfigException;
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
public class ExtractNewRecordStateTest {

    private static final String DROP_TOMBSTONES = "drop.tombstones";
    private static final String HANDLE_DELETES = "delete.handling.mode";
    private static final String OPERATION_HEADER = "operation.header";
    private static final String ADD_SOURCE_FIELDS = "add.source.fields";
    private static final String ROUTE_BY_FIELD = "route.by.field";
    private static final String ADD_FIELDS = "add.fields";
    private static final String ADD_HEADERS = "add.headers";

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

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        final Struct payload = envelope.update(before, after, source, Instant.now());
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

        return operationHeader.next().value().toString();
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
            props.put(OPERATION_HEADER, "true");
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
    @FixFor("DBZ-677")
    public void canUseDeprecatedSmt() {
        try (final UnwrapFromEnvelope<SourceRecord> transform = new UnwrapFromEnvelope<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testAddSourceField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_SOURCE_FIELDS, "lsn");
            transform.configure(props);

            final SourceRecord createRecord = createComplexCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
        }
    }

    @Test
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
    public void testAddFields() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op,lsn");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
        }
    }

    @Test
    public void testAddFieldsSpecifyStruct() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_FIELDS, "op,source.lsn");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(((Struct) unwrapped.value()).get("__source__lsn")).isEqualTo(1234);
        }
    }

    @Test
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
    public void testAddHeaders() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op,lsn");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(2);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
        }
    }

    @Test
    public void testAddHeadersSpecifyStruct() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_HEADERS, "op,source.lsn");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(2);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__source__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
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
    public void testAddSourceFields() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_SOURCE_FIELDS, "lsn,version");
            transform.configure(props);

            final SourceRecord createRecord = createComplexCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).getString("__version")).isEqualTo("version!");
        }
    }

    @Test(expected = ConfigException.class)
    public void testAddSourceNonExistantField() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ADD_SOURCE_FIELDS, "nope");
            transform.configure(props);

            final SourceRecord createRecord = createComplexCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);

            assertThat(((Struct) unwrapped.value()).schema().field("__nope")).isNull();
        }
    }

    @Test
    @FixFor("DBZ-1448")
    public void testAddSourceFieldHandleDeleteRewrite() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_SOURCE_FIELDS, "lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1448")
    public void testAddSourceFieldsHandleDeleteRewrite() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_SOURCE_FIELDS, "lsn,version");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).getString("__version")).isEqualTo("version!");
        }
    }

    @Test
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
            assertThat(((Struct) unwrapped.value()).get("__source__lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1517")
    public void testSchemaChangeEventWithOperationHeader() {
        try (final ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(OPERATION_HEADER, "true");
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
        }
    }
}

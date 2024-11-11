/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.relational.mapping.PropagateSourceMetadataToSchemaParameter.COLUMN_NAME_PARAMETER_KEY;
import static io.debezium.relational.mapping.PropagateSourceMetadataToSchemaParameter.TYPE_LENGTH_PARAMETER_KEY;
import static io.debezium.relational.mapping.PropagateSourceMetadataToSchemaParameter.TYPE_NAME_PARAMETER_KEY;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import io.debezium.connector.AbstractSourceInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.util.Collect;

/**
 * A base abstract class for the Extract-based single message transform tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractExtractStateTest {

    // for ExtractNewRecordState
    protected static final String DROP_TOMBSTONES = "drop.tombstones";
    protected static final String HANDLE_DELETES = "delete.handling.mode";
    protected static final String HANDLE_TOMBSTONE_DELETES = "delete.tombstone.handling.mode";
    protected static final String ROUTE_BY_FIELD = "route.by.field";
    protected static final String ADD_FIELDS = "add.fields";
    protected static final String ADD_HEADERS = "add.headers";
    protected static final String ADD_FIELDS_PREFIX = ADD_FIELDS + ".prefix";
    protected static final String ADD_HEADERS_PREFIX = ADD_HEADERS + ".prefix";
    protected static final String DROP_FIELDS_HEADER_NAME = "drop.fields.header.name";
    protected static final String DROP_FIELDS_FROM_KEY = "drop.fields.from.key";
    protected static final String DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE = "drop.fields.keep.schema.compatible";

    Schema idSchema = SchemaBuilder
            .int8()
            .parameters(Collect.hashMapOf(COLUMN_NAME_PARAMETER_KEY, "id", TYPE_NAME_PARAMETER_KEY, "int"))
            .build();
    Schema nameSchema = SchemaBuilder
            .string()
            .parameters(Collect.hashMapOf(
                    COLUMN_NAME_PARAMETER_KEY, "name",
                    TYPE_NAME_PARAMETER_KEY, "varchar",
                    TYPE_LENGTH_PARAMETER_KEY, "255"))
            .build();
    protected final Schema recordSchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .field("name", nameSchema)
            .build();

    protected final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
            .field("db", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    protected final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    protected SourceRecord createDeleteRecord() {
        final Schema deleteSourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
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
        source.put("db", "test_db");
        source.put("table", "test_table");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", deleteEnvelope.schema(), payload);
    }

    protected SourceRecord createTombstoneRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
    }

    protected SourceRecord createTruncateRecord() {
        final Struct source = createSource();
        source.put("lsn", 1234);
        final Struct truncate = envelope.truncate(source, Instant.ofEpochMilli(System.currentTimeMillis()));
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), truncate);
    }

    protected SourceRecord createCreateRecord() {
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();

        after.put("id", (byte) 1);
        after.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("db", "test_db");
        source.put("table", "test_table");
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    protected SourceRecord createCreateRecordAddingColumn(String columnName, long columnValue) {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field(columnName, Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();

        after.put("id", (byte) 1);
        after.put("name", "myRecord");
        after.put(columnName, columnValue);

        source.put("lsn", 1234);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    protected SourceRecord createHeartbeatRecord() {
        Schema valueSchema = SchemaBuilder.struct()
            .name("io.debezium.connector.common.Heartbeat")
            .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .build();

        Struct value = new Struct(valueSchema);

        Schema keySchema = SchemaBuilder.struct()
            .name("op.with.heartbeat.Key")
            .field("id", Schema.STRING_SCHEMA)
            .build();

        Struct key = new Struct(keySchema).put("id", "123");

        return new SourceRecord(
            new HashMap<>(),
            new HashMap<>(),
            "op.with.heartbeat",
            keySchema,
            key,
            valueSchema,
            value);
    }

    protected SourceRecord createCreateRecordWithOptionalNull() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();

        after.put("id", (byte) 1);
        after.put("name", null);
        source.put("lsn", 1234);
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    protected SourceRecord createCreateRecordWithKey() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .build();

        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct key = new Struct(keySchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();

        key.put("id", (byte) 1);
        after.put("id", (byte) 1);
        after.put("name", null);
        source.put("lsn", 1234);
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    protected SourceRecord createUpdateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        source.put("db", "test_db");
        source.put("table", "test_table");
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    protected SourceRecord createUpdateRecordWithOptionalNull() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        before.put("id", (byte) 1);
        before.put("name", null);
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

    protected SourceRecord createUpdateRecordWithKey() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .build();

        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();
        final Struct key = new Struct(keySchema);
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        key.put("id", (byte) 1);
        before.put("id", (byte) 1);
        before.put("name", null);
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        source.put("db", "test_db");
        source.put("table", "test_table");
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    protected SourceRecord createComplexCreateRecord() {
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
        final Struct source = createSource(sourceSchema);

        before.put("id", (byte) 1);
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    protected SourceRecord createUnknownRecord() {
        final Schema recordSchema = SchemaBuilder.struct().name("unknown")
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    protected SourceRecord createUnknownUnnamedSchemaRecord() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.int8())
                .build();
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
    }

    // for ExtractNewRecordState
    protected String getSourceRecordHeaderByKey(SourceRecord record, String headerKey) {
        Iterator<Header> operationHeader = record.headers().allWithName(headerKey);
        if (!operationHeader.hasNext()) {
            return null;
        }

        Object value = operationHeader.next().value();
        return value != null ? value.toString() : null;
    }

    protected SourceRecord createUpdateRecordWithChangedFields() {
        Envelope changesEnvelope = Envelope.defineSchema()
                .withName("changedFields.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .withSchema(SchemaBuilder.array(Schema.STRING_SCHEMA), "changes")
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);
        final List<String> changes = new ArrayList<>();
        changes.add("name");

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);

        Struct struct = new Struct(changesEnvelope.schema());
        struct.put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
        if (before != null) {
            struct.put(Envelope.FieldName.BEFORE, before);
        }
        struct.put(Envelope.FieldName.AFTER, after);
        if (source != null) {
            struct.put(Envelope.FieldName.SOURCE, source);
        }
        if (Instant.now() != null) {
            struct.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        }

        struct.put("changes", changes);
        struct.put("transaction", transaction);

        final SourceRecord updateRecord = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), struct);
        return updateRecord;
    }

    protected SourceRecord addDropFieldsHeader(SourceRecord record, String name, List<String> values) {
        final Schema dropFieldsSchema = SchemaBuilder
                .array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .optional()
                .name(name)
                .build();
        record.headers().add(name, values, dropFieldsSchema);
        return record;
    }

    protected SourceRecord createCreateRecordWithCreateTime(Instant creationTime) {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .build();

        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct key = new Struct(keySchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = createSource();

        key.put("id", (byte) 1);
        after.put("id", (byte) 1);
        after.put("name", null);
        source.put("lsn", 1234);
        final Struct payload = envelope.create(after, source, creationTime);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    private Struct createSource() {
        return createSource(sourceSchema);
    }

    private Struct createSource(Schema sourceSchema) {
        final Struct source = new Struct(sourceSchema);
        source.put("ts_ms", 1588252618953L);
        source.put("ts_us", 1588252618953000L);
        source.put("ts_ns", 1588252618953000000L);
        return source;
    }
}

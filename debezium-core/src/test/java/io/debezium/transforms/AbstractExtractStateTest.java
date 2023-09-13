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
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;

/**
 * A base abstract class for the Extract-based single message transform tests.
 *
 * @author Chris Cranford
 */
public abstract class AbstractExtractStateTest {

    protected final Schema recordSchema = SchemaBuilder.struct()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();

    protected final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
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

    protected SourceRecord createTombstoneRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
    }

    protected SourceRecord createTruncateRecord() {
        final Struct source = new Struct(sourceSchema);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct truncate = envelope.truncate(source, Instant.ofEpochMilli(System.currentTimeMillis()));
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), truncate);
    }

    protected SourceRecord createCreateRecord() {
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
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
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("name", "myRecord");
        after.put(columnName, columnValue);

        source.put("lsn", 1234);
        source.put("ts_ms", 12836);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
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
        final Struct source = new Struct(sourceSchema);

        after.put("id", (byte) 1);
        after.put("name", null);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
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
        final Struct source = new Struct(sourceSchema);

        key.put("id", (byte) 1);
        after.put("id", (byte) 1);
        after.put("name", null);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    protected SourceRecord createUpdateRecord() {
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

    protected SourceRecord createUpdateRecordWithOptionalNull() {
        final Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", SchemaBuilder.string().optional().defaultValue("default_str").build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
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
        final Struct source = new Struct(sourceSchema);
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
        final Struct source = new Struct(sourceSchema);

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

}

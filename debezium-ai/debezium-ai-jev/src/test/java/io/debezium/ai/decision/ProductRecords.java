/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import java.time.Instant;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;

/**
 * Records of the Debezium tutorial {@code inventory.products} table used by the tests.
 *
 * @author Jiri Pechanec
 */
public final class ProductRecords {

    static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("dbserver1.inventory.products.Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("description", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("dbserver1.inventory.products.Envelope")
            .withRecord(VALUE_SCHEMA)
            .withSource(Schema.STRING_SCHEMA)
            .build();

    static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("dbserver1.inventory.products.Key").field("id", Schema.INT32_SCHEMA).build();

    private ProductRecords() {
    }

    static Struct row(final int id, final String name, final String description) {
        return new Struct(VALUE_SCHEMA).put("id", id).put("name", name).put("description", description);
    }

    private static SourceRecord record(final Struct value) {
        final var key = new Struct(KEY_SCHEMA).put("id", 101);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dbserver1.inventory.products", 2, KEY_SCHEMA, key, ENVELOPE.schema(), value, 1234L,
                new ConnectHeaders());
    }

    public static SourceRecord create(final String name, final String description) {
        return record(ENVELOPE.create(row(101, name, description), null, Instant.now()));
    }

    static SourceRecord update(final String name, final String description) {
        return record(ENVELOPE.update(row(101, "old " + name, "old " + description), row(101, name, description), null, Instant.now()));
    }

    static SourceRecord delete(final String name, final String description) {
        return record(ENVELOPE.delete(row(101, name, description), null, Instant.now()));
    }

    static SourceRecord tombstone() {
        final var key = new Struct(KEY_SCHEMA).put("id", 101);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dbserver1.inventory.products", KEY_SCHEMA, key, null, null);
    }
}

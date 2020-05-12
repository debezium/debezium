/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * @author Jiri Pechanec
 */
public class RouterTest {

    private static final String TOPIC_REGEX = "topic.regex";
    private static final String LANGUAGE = "language";
    private static final String EXPRESSION = "topic.expression";
    private static final String NULL_HANDLING = "null.handling.mode";

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

    @Test(expected = DebeziumException.class)
    public void testExpressionRequired() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailOnInvalidReturnValue() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "1");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            transform.apply(createDeleteRecord(1));
        }
    }

    @Test
    public void shouldRoute() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            assertThat(transform.apply(createDeleteRecord(1)).topic()).isEqualTo("ones");
            assertThat(transform.apply(createDeleteRecord(2)).topic()).isEqualTo("original");
        }
    }

    @Test
    @FixFor("DBZ-2024")
    public void shouldApplyTopicRegex() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(TOPIC_REGEX, "orig.*");
            props.put(EXPRESSION, "value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            assertThat(transform.apply(createDeleteRecord(1)).topic()).describedAs("Matching topic").isEqualTo("ones");
            assertThat(transform.apply(createDeleteCustomerRecord(1)).topic()).describedAs("Non-matching topic").isEqualTo("customer");
        }
    }

    @Test
    public void shouldKeepNulls() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record).topic()).isEqualTo("original");
        }
    }

    @Test
    public void shouldDropNulls() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)");
            props.put(LANGUAGE, "jsr223.groovy");
            props.put(NULL_HANDLING, "drop");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record)).isNull();
        }
    }

    @Test
    public void shouldEvaluateNulls() {
        try (final ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)");
            props.put(LANGUAGE, "jsr223.groovy");
            props.put(NULL_HANDLING, "evaluate");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record).topic()).isEqualTo("nulls");
        }
    }

    private SourceRecord createDeleteRecord(int id) {
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
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "original", envelope.schema(), payload);
    }

    private SourceRecord createDeleteCustomerRecord(int id) {
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
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "customer", envelope.schema(), payload);
    }

    private SourceRecord createNullRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "original", null, null, null, null);
    }
}

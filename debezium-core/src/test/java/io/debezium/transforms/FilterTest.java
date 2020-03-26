/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;

/**
 * @author Jiri Pechanec
 */
public class FilterTest {

    private static final String LANGUAGE = "language";
    private static final String EXPRESSION = "condition";
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
    public void testLanguageRequired() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation == 'd'");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void testExpressionRequired() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(LANGUAGE, "groovy");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailOnUnkownLanguage() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation == 'd'");
            props.put(LANGUAGE, "jython");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailToParseCondition() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation == 'd");
            props.put(LANGUAGE, "groovy");
            transform.configure(props);
        }
    }

    @Test
    public void shouldProcessCondition() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op == 'd' && value.before.id == 2");
            props.put(LANGUAGE, "groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            Assertions.assertThat(transform.apply(createDeleteRecord(2))).isNull();
            Assertions.assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldKeepNulls() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op == 'd' && value.before.id == 2");
            props.put(LANGUAGE, "groovy");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            Assertions.assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldDropNulls() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op == 'd' && value.before.id == 2");
            props.put(LANGUAGE, "groovy");
            props.put(NULL_HANDLING, "drop");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            Assertions.assertThat(transform.apply(record)).isNull();
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldEvaluateNulls() {
        try (final Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op == 'd' && value.before.id == 2");
            props.put(LANGUAGE, "groovy");
            props.put(NULL_HANDLING, "evaluate");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            transform.apply(record);
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
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private SourceRecord createNullRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null, null, null);
    }
}

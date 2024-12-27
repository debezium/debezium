/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * @author Jiri Pechanec
 */
public class FilterTest {

    private static final String TOPIC_REGEX = "topic.regex";
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
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation != 'd'");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void testExpressionRequired() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailOnUnkownLanguage() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation != 'd'");
            props.put(LANGUAGE, "jsr223.jython");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailToParseCondition() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation != 'd");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
        }
    }

    @Test
    public void shouldProcessCondition() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    @FixFor("DBZ-2074")
    public void shouldProcessTopic() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "topic == 'dummy1'");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    @FixFor("DBZ-2074")
    public void shouldProcessHeader() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "headers.idh.value == 1");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    @FixFor("DBZ-2024")
    public void shouldApplyTopicRegex() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(TOPIC_REGEX, "dum.*");
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteCustomerRecord(2);
            assertThat(transform.apply(record)).isSameAs(record);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
        }
    }

    @Test
    public void shouldKeepNulls() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldDropNulls() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.groovy");
            props.put(NULL_HANDLING, "drop");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record)).isNull();
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldEvaluateNulls() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.groovy");
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
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("idh", id);
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy" + id, 0,
                null, null,
                envelope.schema(), payload,
                (long) id,
                headers);
    }

    private SourceRecord createDeleteCustomerRecord(int id) {
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

    private SourceRecord createNullRecord() {
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null, null, null);
    }

    @Test
    public void shouldRunJavaScript() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "value.op != 'd' || value.before.id != 2");
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    @FixFor("DBZ-2074")
    public void shouldRunJavaScriptWithHeaderAndTopic() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "headers.idh.value == 1 && topic.startsWith('dummy')");
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }
}

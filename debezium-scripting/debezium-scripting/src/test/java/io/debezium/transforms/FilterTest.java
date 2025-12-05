/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.transforms.TransformsUtils.createDeleteCustomerRecord;
import static io.debezium.transforms.TransformsUtils.createDeleteRecord;
import static io.debezium.transforms.TransformsUtils.createNullRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.doc.FixFor;

/**
 * @author Jiri Pechanec
 */
public class FilterTest {

    public static final String TOPIC_REGEX = "topic.regex";
    public static final String LANGUAGE = "language";
    public static final String EXPRESSION = "condition";
    public static final String NULL_HANDLING = "null.handling.mode";

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
            props.put(EXPRESSION, "header.idh.value == 1");
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
            props.put(EXPRESSION, "header.idh.value == 1 && topic.startsWith('dummy')");
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }
}

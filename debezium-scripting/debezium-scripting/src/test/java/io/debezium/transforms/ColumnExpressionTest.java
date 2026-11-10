/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.transforms.TransformsUtils.createDeleteRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * @author Felix Dörre
 */
public class ColumnExpressionTest {

    public static final String TOPIC_REGEX = "topic.regex";
    public static final String LANGUAGE = "language";
    public static final String EXPRESSION = "expression";
    public static final String NULL_HANDLING = "null.handling.mode";

    @Test
    void testLanguageRequired() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd'");
                transform.configure(props);
            }
        });
    }

    @Test
    void testExpressionRequired() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(LANGUAGE, "jsr223.groovy");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldFailOnUnkownLanguage() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd'");
                props.put(LANGUAGE, "jsr223.jython");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldFailToParseCondition() {
        assertThrows(DebeziumException.class, () -> {
            try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
                final Map<String, String> props = new HashMap<>();
                props.put(EXPRESSION, "operation != 'd");
                props.put(LANGUAGE, "jsr223.groovy");
                transform.configure(props);
            }
        });
    }

    @Test
    void shouldProcessExpression() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "return [name: row.name+\"-transformed\"]");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name") + "-transformed",
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

    @Test
    void shouldProcessTruncation() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(ColumnExpression.COMPILE_STATIC.name(), "true");
            props.put(EXPRESSION, "return [name: row.getString(\"name\").substring(0,Math.min(row.getString(\"name\").length(),4))]");
            props.put(LANGUAGE, "jsr223.groovy");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name").substring(0, 4),
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

    @Test
    void shouldRunJavaScript() {
        try (ColumnExpression<SourceRecord> transform = new ColumnExpression<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "return {name: value.name+\"-transformed\"}");
            props.put(LANGUAGE, "jsr223.graal.js");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertEquals(((Struct) record.value()).getStruct("before").getString("name") + "-transformed",
                    ((Struct) transform.apply(record).value()).getStruct("before").getString("name"));
        }
    }

}

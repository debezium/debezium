/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.wasm;

import static io.debezium.transforms.TransformsUtils.createAllDataTypeRecord;
import static io.debezium.transforms.TransformsUtils.createArrayDataTypeRecord;
import static io.debezium.transforms.TransformsUtils.createComplexCreateRecord;
import static io.debezium.transforms.TransformsUtils.createDeleteCustomerRecord;
import static io.debezium.transforms.TransformsUtils.createDeleteRecord;
import static io.debezium.transforms.TransformsUtils.createNullRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.transforms.Filter;

public class WasmFilterTest {

    public static final String TOPIC_REGEX = "topic.regex";
    public static final String LANGUAGE = "language";
    public static final String EXPRESSION = "condition";
    public static final String NULL_HANDLING = "null.handling.mode";

    // value.op != 'd' || value.before.id != 2
    private static final String FILTER_1 = filterAbsolutePath("filter1");

    // topic == 'dummy1'
    private static final String FILTER_2 = filterAbsolutePath("filter2");

    // header.idh.value == 1
    private static final String FILTER_3 = filterAbsolutePath("filter3");

    // header.idh.value == 1 && topic.startsWith('dummy')
    private static final String FILTER_4 = filterAbsolutePath("filter4");

    // value.after.id == 1 && value.source.lsn == 1234 && value.source.version == "version!" && topic == "dummy"
    private static final String FILTER_5 = filterAbsolutePath("filter5");

    // all fields matching
    private static final String FILTER_6 = filterAbsolutePath("filter6");

    // array access
    private static final String FILTER_7 = filterAbsolutePath("filter7");

    // keySchema / valueSchema
    private static final String FILTER_8 = filterAbsolutePath("filter8");

    private static String filterAbsolutePath(String filename) {
        return "file:" + new File(".").getAbsolutePath() + "/src/test/resources/wasm/compiled/" + filename + ".wasm";
    }

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
    public void shouldFailOnUnkownLanguage() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "operation != 'd'");
            props.put(LANGUAGE, "wasm.chasm");
            transform.configure(props);
        }
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailToParseCondition() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, "ftp:/filter.wasm");
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
        }
    }

    @Test
    public void shouldProcessConditionWithWasmAot() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldProcessConditionWithWasmInterpreter() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "wasm.chicory-interpreter");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldProcessTopicWithWasm() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldProcessHeader() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_3);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldApplyTopicRegex() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(TOPIC_REGEX, "dum.*");
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "wasm.chicory");
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
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldDropNulls() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "wasm.chicory");
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
            props.put(EXPRESSION, FILTER_1);
            props.put(LANGUAGE, "was,.chicory");
            props.put(NULL_HANDLING, "evaluate");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            transform.apply(record);
        }
    }

    @Test
    public void shouldRunFilterWithHeaderAndTopic() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_4);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(createDeleteRecord(2))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldRunFilterWithComplexCreate() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_5);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createComplexCreateRecord();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldRunFilterWithAllDataTypeSchemas() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_6);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createAllDataTypeRecord(true);
            assertThat(transform.apply(createAllDataTypeRecord(false))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldRunFilterWithArraySchemas() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_7);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createArrayDataTypeRecord(true);
            assertThat(transform.apply(createArrayDataTypeRecord(false))).isNull();
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }

    @Test
    public void shouldAccessSchemaFields() {
        try (Filter<SourceRecord> transform = new Filter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, FILTER_8);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createDeleteRecord(1);
            assertThat(transform.apply(record)).isSameAs(record);
        }
    }
}

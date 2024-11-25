/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.wasm;

import static io.debezium.transforms.TransformsUtils.createComplexCreateRecord;
import static io.debezium.transforms.TransformsUtils.createDeleteCustomerRecord;
import static io.debezium.transforms.TransformsUtils.createDeleteRecord;
import static io.debezium.transforms.TransformsUtils.createMongoDbRecord;
import static io.debezium.transforms.TransformsUtils.createNullRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.transforms.ContentBasedRouter;

public class WasmRouterTest {

    private static final String TOPIC_REGEX = "topic.regex";
    private static final String LANGUAGE = "language";
    private static final String EXPRESSION = "topic.expression";
    private static final String NULL_HANDLING = "null.handling.mode";

    // 1
    private static final String ROUTER_1 = routerAbsolutePath("router1");

    // value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)
    private static final String ROUTER_2 = routerAbsolutePath("router2");

    // value == null ? 'nulls' : ((new groovy.json.JsonSlurper()).parseText(value.after).last_name == 'Kretchmar' ? 'kretchmar' : null)
    private static final String ROUTER_3 = routerAbsolutePath("router3");

    //
    private static final String ROUTER_4 = routerAbsolutePath("router4");

    private static String routerAbsolutePath(String filename) {
        return "file:" + new File(".").getAbsolutePath() + "/src/test/resources/wasm/compiled/" + filename + ".wasm";
    }

    @Test(expected = DebeziumException.class)
    public void shouldFailOnInvalidReturnValue() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_1);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            transform.apply(createDeleteRecord(1));
        }
    }

    @Test
    public void shouldRoute() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            assertThat(transform.apply(createDeleteRecord(1)).topic()).isEqualTo("ones");
            assertThat(transform.apply(createDeleteRecord(2)).topic()).isEqualTo("dummy2");
        }
    }

    @Test
    public void shouldRouteMongoDbFormat() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_3);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            assertThat(transform.apply(createMongoDbRecord()).topic()).isEqualTo("kretchmar");
        }
    }

    @Test
    public void shouldApplyTopicRegex() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(TOPIC_REGEX, "orig.*");
            props.put(EXPRESSION, ROUTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            assertThat(transform.apply(createDeleteRecord(1)).topic()).describedAs("Matching topic").isEqualTo("dummy1");
            assertThat(transform.apply(createDeleteCustomerRecord(1)).topic()).describedAs("Non-matching topic").isEqualTo("customer");
        }
    }

    @Test
    public void shouldKeepNulls() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record).topic()).isEqualTo("dummy");
        }
    }

    @Test
    public void shouldDropNulls() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            props.put(NULL_HANDLING, "drop");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record)).isNull();
        }
    }

    @Test
    public void shouldEvaluateNulls() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_2);
            props.put(LANGUAGE, "wasm.chicory");
            props.put(NULL_HANDLING, "evaluate");
            transform.configure(props);
            final SourceRecord record = createNullRecord();
            assertThat(transform.apply(record).topic()).isEqualTo("nulls");
        }
    }

    @Test
    public void shouldRouteWithComplexCreate() {
        try (ContentBasedRouter<SourceRecord> transform = new ContentBasedRouter<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(EXPRESSION, ROUTER_4);
            props.put(LANGUAGE, "wasm.chicory");
            transform.configure(props);
            assertThat(transform.apply(createComplexCreateRecord()).topic()).isEqualTo("!version");
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.neo4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class Neo4jCdcEventTest {

    @Test
    @DisplayName("a non-Struct value is not recognized and the reason explains a Struct is required")
    void nonStructIsNotRecognized() {
        final var result = Neo4jCdcEvent.from(Map.of("event", "not a struct"));
        assertThat(result.recognized()).isFalse();
        assertThat(result.event()).isNull();
        assertThat(result.skipReason()).contains("not a Struct").contains("EXTENDED");
    }

    @Test
    @DisplayName("a null value is not recognized and the reason names the null type")
    void nullIsNotRecognized() {
        final var result = Neo4jCdcEvent.from(null);
        assertThat(result.recognized()).isFalse();
        assertThat(result.skipReason()).contains("type=undefined");
    }

    @Test
    @DisplayName("a Struct without an 'event' field is not recognized and the reason flags a possible misconfiguration")
    void structWithoutEventIsNotRecognized() {
        final var value = new Struct(SchemaBuilder.struct().name("some.Other").field("x", Schema.STRING_SCHEMA).build())
                .put("x", "y");

        final var result = Neo4jCdcEvent.from(value);
        assertThat(result.recognized()).isFalse();
        assertThat(result.event()).isNull();
        assertThat(result.skipReason()).contains("no 'event' field").contains("some.Other");
    }

    @Test
    @DisplayName("a Struct carrying an 'event' sub-struct is recognized")
    void structWithEventIsRecognized() {
        final var eventSchema = SchemaBuilder.struct().field("eventType", Schema.STRING_SCHEMA).build();
        final var rootSchema = SchemaBuilder.struct().name("neo4j.cdc").field("event", eventSchema).build();
        final var value = new Struct(rootSchema).put("event", new Struct(eventSchema).put("eventType", "NODE"));

        final var result = Neo4jCdcEvent.from(value);
        assertThat(result.recognized()).isTrue();
        assertThat(result.skipReason()).isNull();
        assertThat(result.event().isNode()).isTrue();
    }
}

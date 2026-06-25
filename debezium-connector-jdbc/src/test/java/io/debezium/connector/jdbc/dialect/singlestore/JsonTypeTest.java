/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.Json;

/**
 * Unit tests for the SingleStore {@link JsonType} handler.
 */
@Tag("UnitTests")
class JsonTypeTest {

    @Test
    @DisplayName("Should register for JSON logical name")
    void testRegistrationKeys() {
        assertThat(JsonType.INSTANCE.getRegistrationKeys()).containsExactly(Json.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return simple parameter binding for SingleStore")
    void testQueryBinding() {
        assertThat(JsonType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return null for default value binding")
    void testDefaultValueBinding() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        assertThat(JsonType.INSTANCE.getDefaultValueBinding(schema, "{}")).isNull();
    }

    @Test
    @DisplayName("Should return json as type name for SingleStore")
    void testTypeName() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        assertThat(JsonType.INSTANCE.getTypeName(schema, false)).isEqualTo("json");
        assertThat(JsonType.INSTANCE.getTypeName(schema, true)).isEqualTo("json");
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.Json;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the SingleStore {@link ArrayToJsonType} handler.
 */
@Tag("UnitTests")
class ArrayToJsonTypeTest {

    @Test
    @DisplayName("Should register for ARRAY logical name")
    void testRegistrationKeys() {
        assertThat(ArrayToJsonType.INSTANCE.getRegistrationKeys()).containsExactly("ARRAY");
    }

    @Test
    @DisplayName("Should use a parameter binding for SingleStore JSON columns")
    void testQueryBinding() {
        assertThat(ArrayToJsonType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
    }

    @Test
    @DisplayName("Should serialize a list value to a JSON string before binding")
    void testBindSerializesListToJsonString() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        List<ValueBindDescriptor> bindDescriptors = ArrayToJsonType.INSTANCE.bind(1, schema, List.of("a", 2, true));

        assertThat(bindDescriptors).hasSize(1);
        assertThat(bindDescriptors.get(0).getIndex()).isEqualTo(1);
        assertThat(bindDescriptors.get(0).getValue()).isEqualTo("[\"a\",2,true]");
    }

    @Test
    @DisplayName("Should preserve non-list values for binding")
    void testBindPreservesNonListValues() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        List<ValueBindDescriptor> bindDescriptors = ArrayToJsonType.INSTANCE.bind(2, schema, "plain-value");

        assertThat(bindDescriptors).hasSize(1);
        assertThat(bindDescriptors.get(0).getIndex()).isEqualTo(2);
        assertThat(bindDescriptors.get(0).getValue()).isEqualTo("plain-value");
    }

    @Test
    @DisplayName("Should use the JSON type name")
    void testTypeName() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        assertThat(ArrayToJsonType.INSTANCE.getTypeName(schema, false)).isEqualTo("json");
        assertThat(ArrayToJsonType.INSTANCE.getTypeName(schema, true)).isEqualTo("json");
    }

    @Test
    @DisplayName("Should fail fast when a list cannot be serialized")
    void testBindFailsForUnsupportedListContent() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();
        Object value = List.of(new Object() {
            public String toString() {
                throw new IllegalStateException("boom");
            }
        });

        assertThatThrownBy(() -> ArrayToJsonType.INSTANCE.bind(3, schema, value))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Failed to serialize ARRAY data to JSON");
    }
}

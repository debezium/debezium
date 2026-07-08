/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the StarRocks {@link MapToJsonType} handler.
 */
@Tag("UnitTests")
class MapToJsonTypeTest {

    @Test
    @DisplayName("Should register for MAP schema type")
    void testRegistrationKeys() {
        assertThat(MapToJsonType.INSTANCE.getRegistrationKeys()).containsExactly("MAP");
    }

    @Test
    @DisplayName("Should return json as type name for StarRocks")
    void testTypeName() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();

        assertThat(MapToJsonType.INSTANCE.getTypeName(schema, false)).isEqualTo("json");
    }

    @Test
    @DisplayName("Should bind map values as JSON strings")
    void testBind() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();

        List<ValueBindDescriptor> bindings = MapToJsonType.INSTANCE.bind(0, schema, Map.of("key", "value"));

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("{\"key\":\"value\"}");
    }
}

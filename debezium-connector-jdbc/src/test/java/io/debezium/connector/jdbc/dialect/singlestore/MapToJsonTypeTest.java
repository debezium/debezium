/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the SingleStore {@link MapToJsonType} handler.
 */
@Tag("UnitTests")
class MapToJsonTypeTest {

    @Test
    @DisplayName("Should register for MAP schemas")
    void testRegistrationKeys() {
        assertThat(MapToJsonType.INSTANCE.getRegistrationKeys()).containsExactly("MAP");
    }

    @Test
    @DisplayName("Should bind MAP values as JSON strings")
    void testBindMapToJsonString() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();

        assertThat(MapToJsonType.INSTANCE.bind(1, schema, Map.of("a", "b")))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("{\"a\":\"b\"}");
                });
    }

    @Test
    @DisplayName("Should use SingleStore JSON type name and binding")
    void testJsonTypeDelegation() {
        assertThat(MapToJsonType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
        assertThat(MapToJsonType.INSTANCE.getTypeName(null, false)).isEqualTo("json");
    }
}

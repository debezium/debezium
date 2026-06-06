/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.maria;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.Json;

/**
 * Unit tests for the MariaDB {@link JsonType} handler.
 */
@Tag("UnitTests")
class JsonTypeTest {

    private JsonType jsonType;

    @BeforeEach
    void setUp() {
        jsonType = JsonType.INSTANCE;
    }

    @Test
    @DisplayName("Should register for JSON logical name")
    void testRegistrationKeys() {
        String[] keys = jsonType.getRegistrationKeys();
        assertThat(keys).containsExactly(Json.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return simple parameter binding for MariaDB")
    void testQueryBinding() {
        // MariaDB doesn't support CAST(? AS json), so we use direct binding
        String binding = jsonType.getQueryBinding(null, null, null);
        assertThat(binding).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return null for default value binding")
    void testDefaultValueBinding() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();
        String defaultBinding = jsonType.getDefaultValueBinding(schema, "{}");
        assertThat(defaultBinding).isNull();
    }

    @Test
    @DisplayName("Should return longtext as type name for MariaDB")
    void testTypeName() {
        Schema schema = SchemaBuilder.string().name(Json.LOGICAL_NAME).build();

        String typeNameNonKey = jsonType.getTypeName(schema, false);
        String typeNameKey = jsonType.getTypeName(schema, true);

        // MariaDB JSON is an alias for LONGTEXT
        assertThat(typeNameNonKey).isEqualTo("longtext");
        assertThat(typeNameKey).isEqualTo("longtext");
    }
}

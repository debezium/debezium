/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.Enum;

/**
 * Unit tests for the StarRocks {@link EnumType} handler.
 */
@Tag("UnitTests")
class EnumTypeTest {

    @Test
    @DisplayName("Should register for ENUM logical name")
    void testRegistrationKeys() {
        assertThat(EnumType.INSTANCE.getRegistrationKeys()).containsExactly(Enum.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should map ENUM to string as StarRocks has no ENUM type")
    void testTypeName() {
        Schema schema = SchemaBuilder.string().name(Enum.LOGICAL_NAME).parameter("allowed", "a,b,c").build();

        assertThat(EnumType.INSTANCE.getTypeName(schema, false)).isEqualTo("string");
    }
}

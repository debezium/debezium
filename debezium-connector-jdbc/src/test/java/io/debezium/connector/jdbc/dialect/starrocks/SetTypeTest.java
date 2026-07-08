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

import io.debezium.data.EnumSet;

/**
 * Unit tests for the StarRocks {@link SetType} handler.
 */
@Tag("UnitTests")
class SetTypeTest {

    @Test
    @DisplayName("Should register for SET logical name")
    void testRegistrationKeys() {
        assertThat(SetType.INSTANCE.getRegistrationKeys()).containsExactly(EnumSet.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should map SET to string as StarRocks has no SET type")
    void testTypeName() {
        Schema schema = SchemaBuilder.string().name(EnumSet.LOGICAL_NAME).parameter("allowed", "a,b,c").build();

        assertThat(SetType.INSTANCE.getTypeName(schema, false)).isEqualTo("string");
    }
}

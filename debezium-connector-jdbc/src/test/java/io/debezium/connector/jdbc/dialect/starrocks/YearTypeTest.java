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

import io.debezium.time.Year;

/**
 * Unit tests for the StarRocks {@link YearType} handler.
 */
@Tag("UnitTests")
class YearTypeTest {

    @Test
    @DisplayName("Should register for YEAR logical name")
    void testRegistrationKeys() {
        assertThat(YearType.INSTANCE.getRegistrationKeys()).containsExactly(Year.SCHEMA_NAME);
    }

    @Test
    @DisplayName("Should map YEAR to int as StarRocks has no YEAR type")
    void testTypeName() {
        Schema schema = SchemaBuilder.int32().name(Year.SCHEMA_NAME).build();

        assertThat(YearType.INSTANCE.getTypeName(schema, false)).isEqualTo("int");
    }
}

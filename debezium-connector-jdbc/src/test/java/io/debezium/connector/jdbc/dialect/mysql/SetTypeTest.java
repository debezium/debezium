/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.EnumSet;
import io.debezium.data.EnumeratedValues;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the MySQL {@link SetType} handler.
 */
@Tag("UnitTests")
class SetTypeTest {

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should render escaped set values")
    void shouldRenderEscapedSetValues() {
        final var schema = EnumSet.schema(Arrays.asList("plain", "it's", "back\\slash", "ends\\"));

        assertThat(SetType.INSTANCE.getTypeName(schema, false))
                .isEqualTo("set('plain','it''s','back\\\\slash','ends\\\\')");
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should render escaped set values from allowed parameter")
    void shouldRenderEscapedSetValuesFromAllowedParameter() {
        final String allowedValues = EnumeratedValues.toCommaSeparatedString(
                Arrays.asList("plain", "it's", "back\\slash", "ends\\"));

        assertThat(SetType.INSTANCE.getTypeName(EnumSet.schema(allowedValues), false))
                .isEqualTo("set('plain','it''s','back\\\\slash','ends\\\\')");
    }

    @Test
    @DisplayName("Should render empty set type without allowed values")
    void shouldRenderEmptySetTypeWithoutAllowedValues() {
        assertThat(SetType.INSTANCE.getTypeName(SchemaBuilder.string().name(EnumSet.LOGICAL_NAME).build(), false))
                .isEqualTo("set()");
    }
}

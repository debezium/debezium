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

import io.debezium.data.Enum;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the MySQL {@link EnumType} handler.
 */
@Tag("UnitTests")
class EnumTypeTest {

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should render escaped enum values")
    void shouldRenderEscapedEnumValues() {
        assertThat(EnumType.INSTANCE.getTypeName(Enum.schema(Arrays.asList("plain", "a,b", "it's")), false))
                .isEqualTo("enum('plain','a,b','it''s')");
    }

    @Test
    @FixFor("debezium/dbz#2102")
    @DisplayName("Should render escaped enum values from allowed parameter")
    void shouldRenderEscapedEnumValuesFromAllowedParameter() {
        assertThat(EnumType.INSTANCE.getTypeName(Enum.schema("plain,a\\,b,it's"), false))
                .isEqualTo("enum('plain','a,b','it''s')");
    }

    @Test
    @DisplayName("Should render empty enum type without allowed values")
    void shouldRenderEmptyEnumTypeWithoutAllowedValues() {
        assertThat(EnumType.INSTANCE.getTypeName(SchemaBuilder.string().name(Enum.LOGICAL_NAME).build(), false))
                .isEqualTo("enum()");
    }
}

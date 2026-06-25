/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.vector.FloatVector;

/**
 * Unit tests for the SingleStore {@link FloatVectorType} handler.
 */
@Tag("UnitTests")
class FloatVectorTypeTest {

    @Test
    @DisplayName("Should register for FloatVector logical name")
    void testRegistrationKeys() {
        assertThat(FloatVectorType.INSTANCE.getRegistrationKeys()).containsExactly(FloatVector.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return simple parameter binding for SingleStore")
    void testQueryBinding() {
        assertThat(FloatVectorType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return vector type name with F32 element type for SingleStore")
    void testTypeName() {
        final Schema schema = FloatVector.builder()
                .parameter("__debezium.source.column.length", "3")
                .build();

        assertThat(FloatVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("vector(3, F32)");
    }

    @Test
    @DisplayName("Should use default vector dimension when source column size is not present")
    void testTypeNameDefaultDimension() {
        assertThat(FloatVectorType.INSTANCE.getTypeName(FloatVector.schema(), false)).isEqualTo("vector(2048, F32)");
    }

    @Test
    @DisplayName("Should bind float vector values as JSON array text")
    void testBind() {
        assertThat(FloatVectorType.INSTANCE.bind(1, FloatVector.schema(), List.of(1.0f, 2.0f, 3.0f)))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("[1.0,2.0,3.0]");
                });
    }
}

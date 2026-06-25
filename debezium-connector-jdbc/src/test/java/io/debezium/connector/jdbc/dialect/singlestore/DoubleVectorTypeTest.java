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

import io.debezium.data.vector.DoubleVector;

/**
 * Unit tests for the SingleStore {@link DoubleVectorType} handler.
 */
@Tag("UnitTests")
class DoubleVectorTypeTest {

    @Test
    @DisplayName("Should register for DoubleVector logical name")
    void testRegistrationKeys() {
        assertThat(DoubleVectorType.INSTANCE.getRegistrationKeys()).containsExactly(DoubleVector.LOGICAL_NAME);
    }

    @Test
    @DisplayName("Should return simple parameter binding for SingleStore")
    void testQueryBinding() {
        assertThat(DoubleVectorType.INSTANCE.getQueryBinding(null, null, null)).isEqualTo("?");
    }

    @Test
    @DisplayName("Should return vector type name with F64 element type for SingleStore")
    void testTypeName() {
        final Schema schema = DoubleVector.builder()
                .parameter("__debezium.source.column.length", "3")
                .build();

        assertThat(DoubleVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("vector(3, F64)");
    }

    @Test
    @DisplayName("Should use default vector dimension when source column size is not present")
    void testTypeNameDefaultDimension() {
        assertThat(DoubleVectorType.INSTANCE.getTypeName(DoubleVector.schema(), false)).isEqualTo("vector(16383, F64)");
    }

    @Test
    @DisplayName("Should bind double vector values as JSON array text")
    void testBind() {
        assertThat(DoubleVectorType.INSTANCE.bind(1, DoubleVector.schema(), List.of(1.0d, 2.0d, 3.0d)))
                .singleElement()
                .satisfies(binding -> {
                    assertThat(binding.getIndex()).isEqualTo(1);
                    assertThat(binding.getValue()).isEqualTo("[1.0,2.0,3.0]");
                });
    }
}

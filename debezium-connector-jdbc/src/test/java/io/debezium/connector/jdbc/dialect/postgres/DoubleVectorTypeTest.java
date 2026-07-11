/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.vector.DoubleVector;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link DoubleVectorType} handler.
 */
@Tag("UnitTests")
class DoubleVectorTypeTest {

    @Test
    @DisplayName("Should render a qualified vector type when the source column dimension is propagated")
    void testTypeNameWithDimension() {
        final Schema schema = DoubleVector.builder()
                .parameter("__debezium.source.column.length", "3")
                .build();

        assertThat(DoubleVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("vector(3)");
    }

    @Test
    @DisplayName("Should render an unqualified vector type when the source column dimension is absent")
    void testTypeNameWithoutDimension() {
        assertThat(DoubleVectorType.INSTANCE.getTypeName(DoubleVector.schema(), false)).isEqualTo("vector");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should render an unqualified vector type when the propagated dimension is Integer.MAX_VALUE")
    void testTypeNameWithUnknownDimension() {
        final Schema schema = DoubleVector.builder()
                .parameter("__debezium.source.column.length", String.valueOf(Integer.MAX_VALUE))
                .build();

        assertThat(DoubleVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("vector");
    }
}

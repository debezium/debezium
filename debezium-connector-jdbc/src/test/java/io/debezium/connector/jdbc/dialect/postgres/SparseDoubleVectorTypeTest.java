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

import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link SparseDoubleVectorType} handler.
 */
@Tag("UnitTests")
class SparseDoubleVectorTypeTest {

    @Test
    @DisplayName("Should render a qualified sparsevec type when the source column dimension is propagated")
    void testTypeNameWithDimension() {
        final Schema schema = SparseDoubleVector.builder()
                .parameter("__debezium.source.column.length", "25")
                .build();

        assertThat(SparseDoubleVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("sparsevec(25)");
    }

    @Test
    @DisplayName("Should render an unqualified sparsevec type when the source column dimension is absent")
    void testTypeNameWithoutDimension() {
        assertThat(SparseDoubleVectorType.INSTANCE.getTypeName(SparseDoubleVector.schema(), false)).isEqualTo("sparsevec");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should render an unqualified sparsevec type when the propagated dimension is Integer.MAX_VALUE")
    void testTypeNameWithUnknownDimension() {
        final Schema schema = SparseDoubleVector.builder()
                .parameter("__debezium.source.column.length", String.valueOf(Integer.MAX_VALUE))
                .build();

        assertThat(SparseDoubleVectorType.INSTANCE.getTypeName(schema, false)).isEqualTo("sparsevec");
    }
}

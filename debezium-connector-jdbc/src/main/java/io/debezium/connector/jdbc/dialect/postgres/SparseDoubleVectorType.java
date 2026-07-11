/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.debezium.AbstractSparseDoubleVectorType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation {@link AbstractType} for PGVector's {@code sparsevec} data type, based on
 * the abstract implementation {@link AbstractSparseDoubleVectorType}.
 *
 * A sparse vector is a vector data type that has many dimensions, but only a small proportion
 * of the entries are actually non-zero.
 *
 * The PostgreSQL connector serializes {@code sparsevec} data types using the {@code SparseVector}
 * semantic type, which is a {@link org.apache.kafka.connect.data.Struct} that consists of two
 * fields:
 * <ul>
 *     <li>{@code dimensions} that is an int16 type</li>
 *     <li>{@code vector} that is map where the key is int16 and value is float64 types</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public class SparseDoubleVectorType extends AbstractSparseDoubleVectorType {

    public static SparseDoubleVectorType INSTANCE = new SparseDoubleVectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // pgvector types get dynamically assigned OIDs, so the PostgreSQL driver cannot resolve their
        // dimension and the propagated column length arrives as Integer.MAX_VALUE. Emit an unqualified
        // sparsevec in that case rather than an invalid sparsevec(2147483647). This mirrors BitType,
        // which falls back to "bit varying" for the same Integer.MAX_VALUE sentinel.
        final Optional<String> size = getSourceColumnSize(schema)
                .filter(s -> Integer.parseInt(s) != Integer.MAX_VALUE);
        return size.map(s -> String.format("sparsevec(%s)", s)).orElse("sparsevec");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST(? AS sparsevec)";
    }
}

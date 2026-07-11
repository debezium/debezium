/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.AbstractFloatVectorType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link AbstractFloatVectorType} for PGVector's {@code halfvec} data type.
 *
 * A {@code halfvec} data type is a vector data type that supports storing half-precision vectors.
 *
 * The PostgreSQL connector serializes {@code halfvec} data types using the Debezium semantic type
 * {@link io.debezium.data.vector.FloatVector}, which represents an array of {@code FLOAT32} values.
 *
 * @author Chris Cranford
 */
public class FloatVectorType extends AbstractFloatVectorType {

    public static FloatVectorType INSTANCE = new FloatVectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // pgvector types get dynamically assigned OIDs, so the PostgreSQL driver cannot resolve their
        // dimension and the propagated column length arrives as Integer.MAX_VALUE. Emit an unqualified
        // halfvec in that case rather than an invalid halfvec(2147483647). This mirrors BitType, which
        // falls back to "bit varying" for the same Integer.MAX_VALUE sentinel.
        final Optional<String> size = getSourceColumnSize(schema)
                .filter(s -> Integer.parseInt(s) != Integer.MAX_VALUE);
        return size.map(s -> String.format("halfvec(%s)", s)).orElse("halfvec");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST (? as halfvec)";
    }
}

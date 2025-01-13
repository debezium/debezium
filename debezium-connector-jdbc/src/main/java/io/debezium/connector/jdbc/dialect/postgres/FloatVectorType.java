/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.debezium.AbstractFloatVectorType;

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
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final Optional<String> size = getSourceColumnSize(schema);
        return size.map(s -> String.format("halfvec(%s)", s)).orElse("halfvec");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST (? as halfvec)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (!(value instanceof Collection<?> values)) {
            throw new DebeziumException("Expected value should be a collection");
        }

        return List.of(new ValueBindDescriptor(
                index,
                values.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"))));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.DebeziumException;
import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;

/**
 * An implementation {@link AbstractType} for PGVector's {@code sparsevec} data type.
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
public class SparseVectorType extends AbstractType {

    private static final String DIMENSIONS_FIELD = "dimensions";
    private static final String VECTOR_FIELD = "vector";

    public static SparseVectorType INSTANCE = new SparseVectorType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "io.debezium.data.SparseVector" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final Optional<String> size = getSourceColumnSize(schema);
        return size.map(s -> String.format("sparsevec(%s)", s)).orElse("sparsevec");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST(? AS sparsevec)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (!(value instanceof Struct data)) {
            throw new DebeziumException("Expected value should be a STRUCT data type");
        }

        final int dimensions = data.getInt16(DIMENSIONS_FIELD);
        final Map<Integer, Double> values = data.getMap(VECTOR_FIELD);

        return List.of(new ValueBindDescriptor(
                index,
                String.format("%s/%s",
                        values.entrySet()
                                .stream()
                                .map(e -> e.getKey() + ":" + e.getValue())
                                .collect(Collectors.joining(",", "{", "}")),
                        dimensions)));
    }

}

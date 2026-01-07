/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Abstract base class for Debezium's {@link SparseDoubleVector} logical semantic type.
 *
 * @author Chris Cranford
 */
public abstract class AbstractSparseDoubleVectorType extends AbstractType {
    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ SparseDoubleVector.LOGICAL_NAME };
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        final Struct structValue = requireStruct(value);

        final Short dimensions = structValue.getInt16(SparseDoubleVector.DIMENSIONS_FIELD);
        final Map<Short, Double> vectorMap = structValue.getMap(SparseDoubleVector.VECTOR_FIELD);

        return List.of(new ValueBindDescriptor(index,
                String.format("%s/%s",
                        vectorMap.entrySet().stream()
                                .map(e -> e.getKey() + ":" + e.getValue())
                                .collect(Collectors.joining(",", "{", "}")),
                        dimensions)));
    }
}

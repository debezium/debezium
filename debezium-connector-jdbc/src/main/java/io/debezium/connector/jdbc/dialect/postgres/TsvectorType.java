/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import io.debezium.sink.valuebinding.ValueBindDescriptor;
import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.debezium.AbstractDoubleVectorType;
import io.debezium.sink.column.ColumnDescriptor;

import java.util.List;

/**
 * An implementation of {@link AbstractDoubleVectorType} for PGVector's {@code vector} data type.
 *
 * A {@code vector} data type is a vector data type that supports storing full-precision vectors.
 *
 * The PostgreSQL connector serializes {@code vector} data types using the Debezium semantic type
 * {@link io.debezium.data.vector.DoubleVector|, which represents an arary of {@code FLOAT64} values.
 *
 * @author Chris Cranford
 */
public class TsvectorType extends AbstractType {

    public static TsvectorType INSTANCE = new TsvectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "tsvector";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "io.debezium.data.Tsvector" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as tsvector)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        Object finalValue = value == null ? null : ((String) value).replaceAll("'", "");
        return List.of(new ValueBindDescriptor(index, finalValue));
    }
}

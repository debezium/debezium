/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.debezium.AbstractDoubleVectorType;
import io.debezium.sink.column.ColumnDescriptor;

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
public class DoubleVectorType extends AbstractDoubleVectorType {

    public static DoubleVectorType INSTANCE = new DoubleVectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final Optional<String> size = getSourceColumnSize(schema);
        return size.map(s -> String.format("vector(%s)", s)).orElse("vector");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST(? AS vector)";
    }
}

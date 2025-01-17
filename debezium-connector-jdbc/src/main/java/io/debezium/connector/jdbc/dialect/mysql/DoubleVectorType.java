/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.debezium.AbstractDoubleVectorType;

/**
 * An implementation of {@link AbstractDoubleVectorType} for MySQL's {@code vector} data type.
 *
 * @author Chris Cranford
 */
public class DoubleVectorType extends AbstractDoubleVectorType {

    public static DoubleVectorType INSTANCE = new DoubleVectorType();

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final Optional<String> size = getSourceColumnSize(schema);
        return size.map(s -> String.format("vector(%s)", s)).orElse("vector");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "string_to_vector(?)";
    }
}

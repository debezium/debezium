/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractFloatVectorType;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.FloatVector} types.
 * CockroachDB has no {@code halfvec} type; its native {@code vector} type stores four-byte floats,
 * which matches the float vector element type exactly.
 *
 * @author Virag Tripathi
 */
class FloatVectorType extends AbstractFloatVectorType {

    public static final FloatVectorType INSTANCE = new FloatVectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final Optional<String> size = getSourceColumnSize(schema);
        return size.map(s -> String.format("vector(%s)", s)).orElse("vector");
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "CAST (? as vector)";
    }

}

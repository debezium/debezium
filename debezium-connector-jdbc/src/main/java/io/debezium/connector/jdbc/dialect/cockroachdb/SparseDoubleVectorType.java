/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.AbstractSparseDoubleVectorType;
import io.debezium.data.vector.SparseDoubleVector;

/**
 * An implementation of {@link JdbcType} for {@code io.debezium.data.SparseDoubleVector} types.
 * CockroachDB has no sparse vector type, so this reports the standard unsupported-schema-type
 * error instead of inheriting the PostgreSQL {@code sparsevec} mapping, whose DDL CockroachDB
 * rejects.
 *
 * @author Virag Tripathi
 */
class SparseDoubleVectorType extends AbstractSparseDoubleVectorType {

    public static final SparseDoubleVectorType INSTANCE = new SparseDoubleVectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        throw new ConnectException(
                String.format(
                        "Dialect does not support schema type %s. Please use the VectorToJsonConverter transform in " +
                                "your connector configuration to ingest data of this type.",
                        SparseDoubleVector.LOGICAL_NAME));
    }

}

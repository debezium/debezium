/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

public class SchemaUtils {
    private static final String SCHEMA_PARAMETER_COLUMN_TYPE = "__debezium.source.column.type";
    private static final String SCHEMA_PARAMETER_COLUMN_SIZE = "__debezium.source.column.length";
    private static final String SCHEMA_PARAMETER_COLUMN_PRECISION = "__debezium.source.column.scale";
    private static final String SCHEMA_PARAMETER_COLUMN_NAME = "__debezium.source.column.name";

    public static Optional<String> getSourceColumnType(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_TYPE);
    }

    public static Optional<String> getSourceColumnSize(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_SIZE);
    }

    public static Optional<String> getSourceColumnPrecision(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_PRECISION);
    }

    public static Optional<String> getSourceColumnName(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_NAME);
    }

    public static Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.parameters())) {
            return Optional.ofNullable(schema.parameters().get(parameterName));
        }
        return Optional.empty();
    }
}

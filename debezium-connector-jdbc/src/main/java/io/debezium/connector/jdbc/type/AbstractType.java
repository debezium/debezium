/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * An abstract implementation of {@link Type}, which all types should extend.
 *
 * @author Chris Cranford
 */
public abstract class AbstractType implements Type {

    private static final String QUERY_BINDING = "?";
    private static final String SCHEMA_PARAMETER_COLUMN_TYPE = "__debezium.source.column.type";
    private static final String SCHEMA_PARAMETER_COLUMN_SIZE = "__debezium.source.column.length";
    private static final String SCHEMA_PARAMETER_COLUMN_PRECISION = "__debezium.source.column.scale";

    private DatabaseDialect dialect;

    @Override
    public void configure(JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String getQueryBinding(Schema schema) {
        return QUERY_BINDING;
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        switch (schema.type()) {
            case INT8:
                return Byte.toString((byte) value);
            case INT16:
                return Short.toString((short) value);
            case INT32:
                return Integer.toString((int) value);
            case INT64:
                return Long.toString((long) value);
            case FLOAT32:
                return Float.toString((float) value);
            case FLOAT64:
                return Double.toString((double) value);
            case STRING:
                return "'" + value + "'";
            case BOOLEAN:
                return dialect.getFormattedBoolean((boolean) value);
        }
        throw new IllegalArgumentException(String.format(
                "No default value resolution for schema type %s with name %s and type %s", schema.type(), schema.name(),
                getClass().getName()));
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        query.setParameter(index, value);
    }

    protected DatabaseDialect getDialect() {
        return dialect;
    }

    protected Optional<String> getSourceColumnType(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_TYPE);
    }

    protected Optional<String> getSourceColumnSize(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_SIZE);
    }

    protected Optional<String> getSourceColumnPrecision(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_PRECISION);
    }

    protected Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.parameters())) {
            return Optional.ofNullable(schema.parameters().get(parameterName));
        }
        return Optional.empty();
    }

    protected void throwUnexpectedValue(Object value) {
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value.toString(), value.getClass().getName()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}

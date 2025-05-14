/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.util.SchemaUtils;
import io.debezium.sink.SinkConnectorConfig;

/**
 * An abstract implementation of {@link Type}, which all types should extend.
 *
 * @author Chris Cranford
 */
public abstract class AbstractType implements Type {

    private DatabaseDialect dialect;

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getQueryBindingWithValueCast(column, schema, this);
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
                return "'" + ((String) value).replaceAll("(?<!')'(?!')", "''") + "'";
            case BOOLEAN:
                return dialect.getFormattedBoolean((boolean) value);
        }
        throw new IllegalArgumentException(String.format(
                "No default value resolution for schema type %s with name %s and type %s", schema.type(), schema.name(),
                getClass().getName()));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        return List.of(new ValueBindDescriptor(index, value));
    }

    protected DatabaseDialect getDialect() {
        return dialect;
    }

    protected Optional<String> getSourceColumnType(Schema schema) {
        return SchemaUtils.getSourceColumnType(schema);
    }

    protected Optional<String> getSourceColumnSize(Schema schema) {
        return SchemaUtils.getSourceColumnSize(schema);
    }

    protected Optional<String> getSourceColumnPrecision(Schema schema) {
        return SchemaUtils.getSourceColumnPrecision(schema);
    }

    protected Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.parameters())) {
            return Optional.ofNullable(schema.parameters().get(parameterName));
        }
        return Optional.empty();
    }

    protected void throwUnexpectedValue(Object value) throws ConnectException {
        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value.toString(), value.getClass().getName()));
    }

    protected Struct requireStruct(Object value) throws ConnectException {
        if (value instanceof Struct data) {
            return data;
        }

        throwUnexpectedValue(value);
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}

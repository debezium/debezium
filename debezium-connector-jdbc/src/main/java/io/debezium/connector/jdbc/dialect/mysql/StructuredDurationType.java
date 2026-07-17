/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.type.AbstractTemporalType;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalPreflightValidator;
import io.debezium.connector.jdbc.type.debezium.StructuredTemporalSupport;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDuration;

/**
 * MySQL implementation of {@link StructuredDuration} values.
 */
public class StructuredDurationType extends AbstractTemporalType {

    public static final StructuredDurationType INSTANCE = new StructuredDurationType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ StructuredDuration.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final var kind = StructuredTemporalPreflightValidator.durationKind(schema, null);
        if (!getDialect().getTargetTemporalCapabilities().durationKinds().contains(kind)) {
            throw new org.apache.kafka.connect.errors.ConnectException(String.format(
                    "Structured duration kind '%s' cannot be mapped to MySQL TIME without semantic loss", kind.getValue()));
        }
        return getDialect().getJdbcTypeName(Types.TIME, Size.precision(getSchemaTimePrecision(schema)));
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return "'" + StructuredTemporalLiteral.duration(
                requireStruct(value), getSchemaTimePrecision(schema), getPrecisionLossHandlingMode()) + "'";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        return List.of(new ValueBindDescriptor(index, StructuredTemporalLiteral.duration(
                requireStruct(value), getSchemaTimePrecision(schema), getPrecisionLossHandlingMode()), Types.VARCHAR));
    }

    @Override
    public void validate(ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return;
        }
        final Struct struct = requireStruct(value);
        final var capabilities = getDialect().getTargetTemporalCapabilities();
        StructuredTemporalPreflightValidator.validateDuration(schema, struct, capabilities);
        StructuredTemporalPreflightValidator.validatePrecision(column, struct, capabilities.targetTimePrecision(column), getPrecisionLossHandlingMode());
        StructuredTemporalLiteral.duration(struct,
                capabilities.targetTimePrecision(column), getPrecisionLossHandlingMode());
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, ColumnDescriptor column, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        validate(column, schema, value);
        final int precision = getDialect().getTargetTemporalCapabilities().targetTimePrecision(column);
        return List.of(new ValueBindDescriptor(index,
                StructuredTemporalLiteral.duration(requireStruct(value), precision, getPrecisionLossHandlingMode()), Types.VARCHAR));
    }

    private int getSchemaTimePrecision(Schema schema) {
        final int maxPrecision = getDialect().getTargetTemporalCapabilities().maxTimePrecision();
        final int precision = StructuredTemporalSupport.getPrecision(schema).orElse(Math.min(6, maxPrecision));
        return Math.min(precision, maxPrecision);
    }
}

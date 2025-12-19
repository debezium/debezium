/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link VariableScaleDecimal} values.
 *
 * @author Chris Cranford
 */
public class VariableScaleDecimalType extends AbstractType {

    public static final VariableScaleDecimalType INSTANCE = new VariableScaleDecimalType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ VariableScaleDecimal.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // The data passed by VariableScaleDecimal data types does not provide adequate information to
        // resolve the precision and scale for the data type, so instead we're going to default to the
        // maximum double-based data types for the dialect, using DOUBLE.
        return getDialect().getJdbcTypeName(Types.DOUBLE);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Struct) {
            Optional<BigDecimal> bigDecimalValue = VariableScaleDecimal.toLogical((Struct) value).getDecimalValue();
            return List.of(new ValueBindDescriptor(index, bigDecimalValue.orElseThrow()));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}

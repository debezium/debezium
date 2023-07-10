/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.VariableScaleDecimal;

/**
 * An implementation of {@link Type} for {@link VariableScaleDecimal} values.
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
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // The data passed by VariableScaleDecimal data types does not provide adequate information to
        // resolve the precision and scale for the data type, so instead we're going to default to the
        // maximum double-based data types for the dialect, using DOUBLE.
        return dialect.getTypeName(Types.DOUBLE);
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Struct) {
            Optional<BigDecimal> bigDecimalValue = VariableScaleDecimal.toLogical((Struct) value).getDecimalValue();
            query.setParameter(index, bigDecimalValue.orElseThrow());
        }
        else {
            throwUnexpectedValue(value);
        }
        return 1;
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} for {@link Decimal} values.
 *
 * @author Chris Cranford
 */
public class ConnectDecimalType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectDecimalType.class);

    public static final ConnectDecimalType INSTANCE = new ConnectDecimalType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Decimal.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        DatabaseDialect dialect = getDialect();
        int scale = Integer.parseInt(getSchemaParameter(schema, "scale").orElse("0"));
        if (scale < 0 && !dialect.isNegativeScaleAllowed()) {
            // Oracle submits negative scale values where the bound value will be rounded based on the scale.
            // This means when replicating to non-Oracle systems, negative scale values need to be omitted
            // from the types as they're not supported.
            LOGGER.warn("JdbcType {} detected with negative scale {}, using scale 0 instead.", schema.name(), scale);
            scale = 0;
        }

        int precision = Integer.parseInt(getSchemaParameter(schema, "connect.decimal.precision").orElse("0"));
        if (precision > 0) {
            // Some dialects may have a smaller precision than what's provided by the source.
            // In such cases, we apply the dialect's upper-bounds.
            precision = Math.min(precision, dialect.getDefaultDecimalPrecision());
            return dialect.getJdbcTypeName(Types.DECIMAL, Size.precision(precision, scale));
        }
        else if (scale != 0) {
            return dialect.getJdbcTypeName(Types.DECIMAL, Size.precision(dialect.getDefaultDecimalPrecision(), scale));
        }
        return dialect.getJdbcTypeName(Types.DECIMAL);
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value instanceof Number) {
            return value.toString();
        }
        throwUnexpectedValue(value);
        return null;
    }
}

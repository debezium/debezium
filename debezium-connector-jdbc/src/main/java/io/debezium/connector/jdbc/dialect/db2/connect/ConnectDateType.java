/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.connect;

import java.util.List;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractDateType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectDateType extends AbstractDateType {

    public static final ConnectDateType INSTANCE = new ConnectDateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Date.LOGICAL_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedDate(DateTimeUtils.toLocalDateFromDate((java.util.Date) value));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof java.util.Date) {
            return List.of(new ValueBindDescriptor(index, java.sql.Date.valueOf(DateTimeUtils.toLocalDateFromDate((java.util.Date) value))));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}

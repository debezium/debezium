/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractDateType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.Date;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class DateType extends AbstractDateType {

    public static final DateType INSTANCE = new DateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Date.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedDate(DateTimeUtils.toLocalDateOfEpochDays(((Number) value).longValue()));
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Number) {
            query.setParameter(index, DateTimeUtils.toLocalDateOfEpochDays(((Number) value).longValue()));
        }
        else {
            throwUnexpectedValue(value);
        }
    }

}

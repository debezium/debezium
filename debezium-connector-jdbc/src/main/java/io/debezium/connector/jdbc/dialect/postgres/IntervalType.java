/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.MicroDuration;

/**
 * An implementation of {@link Type} for {@link MicroDuration} types.
 *
 * @author Chris Cranford
 */
class IntervalType extends AbstractType {

    public static final IntervalType INSTANCE = new IntervalType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroDuration.SCHEMA_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as interval)";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "interval";
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        if (value instanceof Long) {
            final double doubleValue = ((Long) value).doubleValue() / 1_000_000d;
            return String.format("'%d seconds'", (long) doubleValue);
        }
        // apply no default
        return null;
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {
        if (value != null && Long.class.isAssignableFrom(value.getClass())) {
            final double doubleValue = ((Long) value).doubleValue() / 1_000_000d;
            query.setParameter(index, ((long) doubleValue) + " seconds");
        }
        else {
            query.setParameter(index, value);
        }

        return 1;
    }
}

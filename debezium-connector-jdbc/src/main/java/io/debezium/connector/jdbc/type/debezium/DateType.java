/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.Date;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class DateType extends AbstractType {

    public static final DateType INSTANCE = new DateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Date.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return dialect.getTypeName(Types.DATE);
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedDate(getInstantFromEpochDays((Integer) value).atZone(ZoneOffset.UTC));
        // final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
        // final String result = formatter.format(getInstantFromEpochDays((Integer) value).atZone(ZoneOffset.UTC));
        // if (dialect instanceof OracleDatabaseDialect) {
        // return String.format("TO_DATE('%s', 'YYYY-MM-DD')", result);
        // }
        // return String.format("'%s'", result);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Integer) {
            query.setParameter(index, java.util.Date.from(getInstantFromEpochDays((Integer) value)));
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    private static Instant getInstantFromEpochDays(Integer value) {
        return Instant.ofEpochSecond(value.longValue() * 86400L);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends AbstractTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTimestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimestampWithTimeZone((String) value);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof String) {
            final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER)
                    .withZoneSameInstant(getDatabaseTimeZone().toZoneId());
            query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITH_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    @Override
    protected int getJdbcType() {
        return Types.TIMESTAMP_WITH_TIMEZONE;
    }

}

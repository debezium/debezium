/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;

/**
 * An abstract Debezium time-type implementation of {@link AbstractTimeType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumTimeType extends AbstractTimeType {

    @Override
    public String getQueryBinding(Schema schema) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        final LocalTime localTime = getLocalTime((Number) value);
        if (dialect.isConnectionTimeZoneSet() || dialect.isJdbcTimeZoneSet()) {
            return getDialect().getFormattedDateTime(localTime.atDate(LocalDate.now()).atZone(getDatabaseTimeZone().toZoneId()));
        }
        return dialect.getFormattedTime(localTime);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Number) {
            final LocalTime localTime = getLocalTime((Number) value);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            if (getDialect().isConnectionTimeZoneSet() || getDialect().isJdbcTimeZoneSet()) {
                query.setParameter(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId()));
            }
            else {
                query.setParameter(index, localDateTime);
            }
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    protected abstract LocalTime getLocalTime(Number value);

}

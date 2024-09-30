/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.MicroTime;

/**
 * An implementation of {@link Type} for {@link MicroTime} values.
 *
 * @author Chris Cranford
 */
public class MicroTimeType extends AbstractTimeType {

    public static final MicroTimeType INSTANCE = new MicroTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroTime.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTime(toZonedDateTime((long) value));
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Long) {
            query.setParameter(index, toZonedDateTime((long) value), StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    private ZonedDateTime toZonedDateTime(long value) {
        final Duration duration = Duration.of(value, ChronoUnit.MICROS);
        return Instant.EPOCH.plus(duration.toNanos(), ChronoUnit.NANOS).atZone(getDatabaseTimeZone().toZoneId());
    }
}

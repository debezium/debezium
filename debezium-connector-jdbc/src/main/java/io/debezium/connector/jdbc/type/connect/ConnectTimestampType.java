/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectTimestampType extends AbstractTimestampType {

    public static final ConnectTimestampType INSTANCE = new ConnectTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.LOGICAL_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimestamp(DateTimeUtils.toZonedDateTimeFromDate((java.util.Date) value, ZoneOffset.UTC));
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof java.util.Date) {
            final LocalDateTime localDateTime = DateTimeUtils.toLocalDateTimeFromDate((java.util.Date) value);
            if (getDialect().isTimeZoneSet()) {
                return List.of(new ValueBindDescriptor(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId())));
            }
            return List.of(new ValueBindDescriptor(index, localDateTime));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}

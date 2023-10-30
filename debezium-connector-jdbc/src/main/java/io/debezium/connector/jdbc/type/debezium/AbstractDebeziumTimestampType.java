/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.type.AbstractTimestampType;

/**
 * An abstract Debezium timestamp-type implementation of {@link AbstractTimestampType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumTimestampType extends AbstractTimestampType {

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Number) {

            final ZonedDateTime zonedDateTime = getLocalDateTime(((Number) value).longValue()).atZone(ZoneOffset.UTC);

            if (getDialect().isTimeZoneSet()) {
                return List.of(new ValueBindDescriptor(index,
                        getDialect().convertToCorrectDateTime(zonedDateTime.withZoneSameInstant(getDatabaseTimeZone().toZoneId())),
                        getDialect().getTimestampType().orElse(null)));
            }

            return List.of(new ValueBindDescriptor(index, zonedDateTime));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected abstract LocalDateTime getLocalDateTime(long value);

}

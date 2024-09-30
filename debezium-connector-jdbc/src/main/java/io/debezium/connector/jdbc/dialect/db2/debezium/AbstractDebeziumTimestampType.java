/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.debezium;

import java.sql.Timestamp;
import java.time.LocalDateTime;
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

            final LocalDateTime localDateTime = getLocalDateTime(((Number) value).longValue());

            if (getDialect().isTimeZoneSet()) {
                return List.of(new ValueBindDescriptor(index,
                        Timestamp.valueOf(localDateTime.atZone(getDatabaseTimeZone().toZoneId()).toLocalDateTime())));
            }

            return List.of(new ValueBindDescriptor(index, Timestamp.valueOf(localDateTime)));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected abstract LocalDateTime getLocalDateTime(long value);

}

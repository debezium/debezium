/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.LocalDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.type.AbstractTimestampType;

/**
 * An abstract Debezium timestamp-type implementation of {@link AbstractTimestampType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractDebeziumTimestampType extends AbstractTimestampType {

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Number) {
            final LocalDateTime localDateTime = getLocalDateTime(((Number) value).longValue());
            if (getDialect().isTimeZoneSet()) {
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

    protected abstract LocalDateTime getLocalDateTime(long value);

}

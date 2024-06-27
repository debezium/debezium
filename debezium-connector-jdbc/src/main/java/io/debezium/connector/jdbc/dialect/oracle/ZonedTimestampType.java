/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.time.ZonedDateTime;
import java.util.List;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends io.debezium.connector.jdbc.type.debezium.ZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    protected List<ValueBindDescriptor> infinityTimestampValue(int index, Object value) {
        final ZonedDateTime zdt;

        if (POSITIVE_INFINITY.equals(value)) {
            zdt = ZonedDateTime.parse(getDialect().getTimestampPositiveInfinityValue(), ZonedTimestamp.FORMATTER);
        }
        else {
            zdt = ZonedDateTime.parse(getDialect().getTimestampNegativeInfinityValue(), ZonedTimestamp.FORMATTER);
        }

        return List.of(new ValueBindDescriptor(index, zdt, getJdbcBindType()));
    }

    @Override
    protected List<ValueBindDescriptor> normalTimestampValue(int index, Object value) {

        final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).withZoneSameInstant(getDatabaseTimeZone().toZoneId());

        return List.of(new ValueBindDescriptor(index, zdt, getJdbcBindType()));
    }
}

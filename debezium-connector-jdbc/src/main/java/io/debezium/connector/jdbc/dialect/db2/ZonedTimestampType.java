/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends DebeziumZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    protected List<ValueBindDescriptor> infinityTimestampValue(int index, Object value) {
        final ZonedDateTime zdt;

        if (POSITIVE_INFINITY.equals(value)) {
            zdt = ZonedDateTime.parse(getDialect().getTimestampPositiveInfinityValue(), ZonedTimestamp.FORMATTER);
        }
        else {
            zdt = ZonedDateTime.parse(getDialect().getTimestampNegativeInfinityValue(), ZonedTimestamp.FORMATTER);
        }

        return List.of(new ValueBindDescriptor(index, Timestamp.valueOf((zdt.toLocalDateTime()))));
    }

    @Override
    protected List<ValueBindDescriptor> normalTimestampValue(int index, Object value) {

        final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).withZoneSameInstant(getDatabaseTimeZone().toZoneId());

        return List.of(new ValueBindDescriptor(index, Timestamp.from(zdt.toInstant())));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link JdbcType} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends DebeziumZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    protected int getJdbcBindType() {
        return Types.TIMESTAMP; // TIMESTAMP_WITH_TIMEZONE not supported
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        if (value == null) {
            return super.getDefaultValueBinding(schema, value);
        }

        final ZonedDateTime parsed = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
        final ZonedDateTime clamped = clampToDialectBounds(parsed);
        if (clamped.toInstant().equals(parsed.toInstant())) {
            return getDialect().getFormattedTimestampWithTimeZone(ZonedTimestamp.toIsoString(parsed, null, null));
        }
        return String.format("'%s'", ((MySqlDatabaseDialect) getDialect()).toSessionZoneLiteral(clamped));
    }

    private ZonedDateTime clampToDialectBounds(ZonedDateTime value) {
        final ZonedDateTime minimum = ZonedDateTime.parse(getDialect().getTimestampNegativeInfinityValue(), ZonedTimestamp.FORMATTER);
        if (value.toInstant().isBefore(minimum.toInstant())) {
            return minimum;
        }
        final ZonedDateTime maximum = ZonedDateTime.parse(getDialect().getTimestampPositiveInfinityValue(), ZonedTimestamp.FORMATTER);
        if (value.toInstant().isAfter(maximum.toInstant())) {
            return maximum;
        }
        return value;
    }
}

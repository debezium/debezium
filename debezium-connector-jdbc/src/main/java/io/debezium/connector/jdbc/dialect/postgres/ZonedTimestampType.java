/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values specific to PostgreSQL.
 *
 * @author Mario Fiore Vitale
 */
public class ZonedTimestampType extends io.debezium.connector.jdbc.type.debezium.ZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();
    public static final List<String> POSITIVE_INFINITY = List.of("infinity", "+infinity");
    public static final String NEGATIVE_INFINITY = "-infinity";

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {

        if (POSITIVE_INFINITY.contains(value) || NEGATIVE_INFINITY.equals(value)) {
            return "cast(? as timestamptz)";
        }

        return super.getQueryBinding(column, schema, value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof String) {

            final ZonedDateTime zdt;

            if (POSITIVE_INFINITY.contains(value)) {
                return List.of(new ValueBindDescriptor(index, POSITIVE_INFINITY.get(0), Types.VARCHAR));
            }

            if (NEGATIVE_INFINITY.equals(value)) {
                return List.of(new ValueBindDescriptor(index, NEGATIVE_INFINITY, Types.VARCHAR));
            }

            zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).withZoneSameInstant(getDatabaseTimeZone().toZoneId());

            return List.of(new ValueBindDescriptor(index, zdt.toOffsetDateTime(), getJdbcType()));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Types;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends AbstractTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTimestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimestampWithTimeZone((String) value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof String) {
            final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).withZoneSameInstant(getDatabaseTimeZone().toZoneId());
            // TODO check if this works with PreparedStatement
            if(getDialect().getTimestampType().isPresent()) {
                return List.of(new ValueBindDescriptor(index, getDialect().convertToCorrectDateTime(zdt), getDialect().getTimestampType().get()));
            }

            return List.of(new ValueBindDescriptor(index, getDialect().convertToCorrectDateTime(zdt)));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    @Override
    protected int getJdbcType() {
        return Types.TIMESTAMP_WITH_TIMEZONE;
    }

}

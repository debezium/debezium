/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.ValueBindDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link Type} for {@link ZonedTime} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimeType extends AbstractTimeType {

    public static final ZonedTimeType INSTANCE = new ZonedTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        // NOTE:
        // The MySQL connector does not use the __debezium.source.column.scale parameter to pass
        // the time column's precision but instead uses the __debezium.source.column.length key
        // which differs from all other connector implementations.
        //
        final int precision = getTimePrecision(schema);

        // We use TIMESTAMP here even for source TIME types as Oracle will use DATE types for
        // such columns, and it only supports second-based precision. By using TIMESTAMP, the
        // precision best aligns with the potential of up to 6.
        if (precision > 0) {
            return dialect.getTypeName(getJdbcType(), Size.precision(precision));
        }

        // We use the max dialect precision here as nanosecond precision is only permissible by specific
        // dialects and this handles situations of rounding values to the nearest precision of the value is
        // sourced from a source with a higher dialect.
        return dialect.getTypeName(getJdbcType(), Size.precision(dialect.getMaxTimePrecision()));
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimeWithTimeZone((String) value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (value instanceof String) {
            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER).atDate(LocalDate.now()).toZonedDateTime();

            if (getDialect().isTimeZoneSet()) {
                if (getDialect().shouldBindTimeWithTimeZoneAsDatabaseTimeZone()) {
                    return List
                            .of(new ValueBindDescriptor(index, zdt.withZoneSameInstant(getDatabaseTimeZone().toZoneId())));
                }

                return List.of(new ValueBindDescriptor(index, zdt, getJdbcType()));
            }
            return List.of(new ValueBindDescriptor(index, zdt));

        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected int getJdbcType() {
        return Types.TIME_WITH_TIMEZONE;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.sqlserver.SqlServerDatabaseDialect;
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
            return dialect.getTypeName(getJdbcType(dialect), Size.precision(precision));
        }

        // We use the max dialect precision here as nanosecond precision is only permissible by specific
        // dialects and this handles situations of rounding values to the nearest precision of the value is
        // sourced from a source with a higher dialect.
        return dialect.getTypeName(getJdbcType(dialect), Size.precision(dialect.getMaxTimePrecision()));
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimeWithTimeZone((String) value);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof String) {
            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER).atDate(LocalDate.now()).toZonedDateTime();
            if (getDialect().isJdbcTimeZoneSet()) {
                query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITH_TIMEZONE);
            }
            else {
                if (getDialect().isConnectionTimeZoneSet()) {
                    query.setParameter(index, zdt.withZoneSameInstant(getDatabaseTimeZone().toZoneId()));
                }
                else if (getDialect() instanceof SqlServerDatabaseDialect) {
                    query.setParameter(index, zdt.toLocalDateTime());
                }
                else {
                    query.setParameter(index, zdt);
                }
            }
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    private int getJdbcType(DatabaseDialect dialect) {
        if (dialect instanceof SqlServerDatabaseDialect) {
            // SQL Server does not support time with time zone, but to align the behavior with other dialects,
            // we will directly map to a TIMESTAMP with TIME ZONE so that SQL Server is mapped to DATETIMEOFFSET.
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        return Types.TIME_WITH_TIMEZONE;
    }
}

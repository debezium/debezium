/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2.debezium;

import java.sql.Types;
import java.time.LocalTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.NanoTime;

/**
 * An implementation of {@link Type} for {@link NanoTime} values.
 *
 * @author Chris Cranford
 */
public class NanoTimeType extends AbstractDebeziumTimeType {

    public static final NanoTimeType INSTANCE = new NanoTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ NanoTime.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // NOTE:
        // The MySQL connector does not use the __debezium.source.column.scale parameter to pass
        // the time column's precision but instead uses the __debezium.source.column.length key
        // which differs from all other connector implementations.
        //
        int precision = getTimePrecision(schema);
        DatabaseDialect dialect = getDialect();
        // We use TIMESTAMP here even for source TIME types as Oracle will use DATE types for
        // such columns, and it only supports second-based precision. By using TIMESTAMP, the
        // precision best aligns with the potential of up to 6.
        if (precision > 0) {
            precision = Math.min(precision, dialect.getMaxTimePrecision());
            return dialect.getJdbcTypeName(Types.TIME, Size.precision(precision));
        }

        // We use the max dialect precision here as nanosecond precision is only permissible by specific
        // dialects and this handles situations of rounding values to the nearest precision of the value is
        // sourced from a source with a higher dialect.
        return dialect.getJdbcTypeName(Types.TIME, Size.precision(dialect.getMaxTimePrecision()));
    }

    @Override
    protected LocalTime getLocalTime(Number value) {
        return DateTimeUtils.toLocalTimeFromDurationNanoseconds(value.longValue());
    }

}

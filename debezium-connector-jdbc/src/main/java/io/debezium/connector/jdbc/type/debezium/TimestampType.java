/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.Timestamp;

/**
 * An implementation of {@link Type} for {@link Timestamp} values.
 *
 * @author Chris Cranford
 */
public class TimestampType extends AbstractTimestampType {

    public static final TimestampType INSTANCE = new TimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final int precision = getTimePrecision(schema);
        if (precision > 0 && precision <= dialect.getMaxTimestampPrecision()) {
            return dialect.getTypeName(Types.TIMESTAMP, Size.precision(precision));
        }
        return dialect.getTypeName(Types.TIMESTAMP);
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        final ZonedDateTime zdt = Instant.ofEpochMilli((long) value).atZone(ZoneOffset.UTC);
        return dialect.getFormattedDateTime(zdt);
        // if (dialect instanceof OracleDatabaseDialect) {
        // final String result = DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zdt);
        // return String.format("TO_TIMESTAMP('%s', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6 TZH:TZM')", result);
        // }
        // else if (dialect instanceof MySqlDatabaseDialect) {
        // final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        // .parseCaseInsensitive()
        // .append(DateTimeFormatter.ISO_LOCAL_DATE)
        // .appendLiteral(' ')
        // .append(DateTimeFormatter.ISO_LOCAL_TIME)
        // .toFormatter();
        // return String.format("'%s'", formatter.format(zdt));
        // }
        // else if (dialect instanceof Db2DatabaseDialect) {
        // final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        // .parseCaseInsensitive()
        // .append(DateTimeFormatter.ISO_LOCAL_DATE)
        // .appendLiteral(' ')
        // .append(DateTimeFormatter.ISO_LOCAL_TIME)
        // .toFormatter();
        // return String.format("'%s'", formatter.format(zdt));
        // }
        // return String.format("'%s'", DateTimeFormatter.ISO_DATE_TIME.format(zdt));
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Long) {
            final ZonedDateTime zdt = Instant.ofEpochMilli((long) value).atZone(ZoneOffset.UTC);
            query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

}

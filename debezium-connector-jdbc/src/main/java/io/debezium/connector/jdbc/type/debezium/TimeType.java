/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.Time;

/**
 * An implementation of {@link Type} for {@link Time} values.
 *
 * @author Chris Cranford
 */
public class TimeType extends AbstractTimeType {

    public static final TimeType INSTANCE = new TimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTime(toZonedDateTime((int) value));
        // final ZonedDateTime zdt = toZonedDateTime((int) value);
        // if (dialect instanceof OracleDatabaseDialect) {
        // final String result = DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zdt);
        // return String.format("TO_TIMESTAMP('%s', 'YYYY-MM-DD\"T\"HH24:MI::SS.FF9 TZH:TZM')", result);
        // }
        // else if (dialect instanceof PostgresDatabaseDialect) {
        // return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zdt));
        // }
        // else if (dialect instanceof MySqlDatabaseDialect) {
        // return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zdt));
        // }
        // else if (dialect instanceof Db2DatabaseDialect) {
        // return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zdt));
        // }
        // return String.format("'%s'", DateTimeFormatter.ISO_TIME.format(zdt));
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Integer) {
            final ZonedDateTime zdt = toZonedDateTime((int) value);
            query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    private ZonedDateTime toZonedDateTime(int value) {
        return Instant.EPOCH.plus(value, ChronoUnit.MILLIS).atZone(ZoneOffset.UTC);
    }

}

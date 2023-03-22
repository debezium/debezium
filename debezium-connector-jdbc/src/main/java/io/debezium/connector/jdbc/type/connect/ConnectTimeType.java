/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@link org.apache.kafka.connect.data.Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectTimeType extends AbstractTimeType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.LOGICAL_NAME };
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTime(toZonedDateTime((Date) value));
        // final ZonedDateTime zdt = toZonedDateTime((Date) value);
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
        else if (value instanceof Date) {
            final ZonedDateTime zdt = toZonedDateTime((Date) value);
            query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    private ZonedDateTime toZonedDateTime(Date value) {
        return value.toInstant().atZone(ZoneOffset.UTC);
    }
}

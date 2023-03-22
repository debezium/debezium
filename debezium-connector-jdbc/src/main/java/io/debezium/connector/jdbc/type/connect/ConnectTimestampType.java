/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectTimestampType extends AbstractType {

    public static final ConnectTimestampType INSTANCE = new ConnectTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.LOGICAL_NAME };
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
        final ZonedDateTime zdt = ((java.util.Date) value).toInstant().atZone(ZoneOffset.UTC);
        return dialect.getFormattedTimestamp(zdt);
        // final String result = DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zdt);
        // if (dialect instanceof OracleDatabaseDialect) {
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
        // return String.format("'%s'", result);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof java.util.Date) {
            final ZonedDateTime zdt = ((java.util.Date) value).toInstant().atZone(ZoneOffset.UTC);
            query.setParameter(index, zdt, StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE);
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import java.sql.Types;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.query.BindableType;
import org.hibernate.query.Query;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimestampType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.time.ZonedTimestamp;

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
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        int precision = getTimePrecision(schema);
        if (precision > 0 && precision < dialect.getMaxTimestampPrecision()) {
            return dialect.getTypeName(getJdbcType(), Size.precision(precision));
        }
        return dialect.getTypeName(getJdbcType());
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTimestampWithTimeZone((String) value);
        // if (dialect instanceof OracleDatabaseDialect) {
        // return String.format("TO_TIMESTAMP_TZ('%s', 'YYYY-MM-DD\"T\"HH24:MI::SS.FF9 TZH:TZM')", value);
        // }
        // else if (dialect instanceof MySqlDatabaseDialect || dialect instanceof Db2DatabaseDialect) {
        // final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
        // return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zdt));
        // }
        // return String.format("'%s'", value);
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof String) {
            final ZonedDateTime zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
            query.setParameter(index, zdt, getBindableType());
        }
        else {
            throwUnexpectedValue(value);
        }
    }

    protected int getJdbcType() {
        return Types.TIMESTAMP_WITH_TIMEZONE;
    }

    protected BindableType<ZonedDateTime> getBindableType() {
        return StandardBasicTypes.ZONED_DATE_TIME_WITH_TIMEZONE;
    }
}

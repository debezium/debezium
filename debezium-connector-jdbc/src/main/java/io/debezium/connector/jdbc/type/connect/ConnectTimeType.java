/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.util.DateTimeUtils;

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
    public String getQueryBinding(ColumnDescriptor column, Schema schema) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedTime(DateTimeUtils.toZonedDateTimeFromDate((Date) value, getDatabaseTimeZone()));
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof Date) {
            final LocalTime localTime = DateTimeUtils.toLocalTimeFromUtcDate((Date) value);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            if (getDialect().isTimeZoneSet()) {
                query.setParameter(index, localDateTime.atZone(getDatabaseTimeZone().toZoneId()));
            }
            else {
                // NOTE
                // ----
                // Hibernate truncates LocalTime to 0 precision, see LocalTimeJavaType#getDefaultSqlPrecision.
                // To avoid this loss in precision from the source system, the following will bind the value
                // as a LocalDateTime using the current date as the base in order to avoid data loss.
                query.setParameter(index, localDateTime);
            }
        }
        else {
            throwUnexpectedValue(value);
        }
        return 1;
    }

}

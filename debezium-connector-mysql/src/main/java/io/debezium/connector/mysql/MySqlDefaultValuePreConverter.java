/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;

/**
 * This class is used by a DDL parser to convert the string default value to a Java type
 * recognized by value converters for a subset of types. The functionality is kept separate
 * from the main converters to centralize the formatting logic if necessary.
 *
 * @author Jiri Pechanec
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public class MySqlDefaultValuePreConverter  {

    private static final String ALL_ZERO_TIMESTAMP = "0000-00-00 00:00:00";

    private static final String ALL_ZERO_DATE = "0000-00-00";

    private static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:00";

    private static final String EPOCH_DATE = "1970-01-01";

    /**
     * Converts a default value from the expected format to a logical object acceptable by the main JDBC
     * converter.
     *
     * @param column column definition
     * @param value string formatted default value
     * @return value converted to a Java type
     */
    public Object convert(Column column, String value) {
        if (value == null) {
            return value;
        }
        switch (column.jdbcType()) {
        case Types.DATE:
            if (ALL_ZERO_DATE.equals(value)) value = EPOCH_DATE;
            return LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE.parse(value));
        case Types.TIMESTAMP:
            if (ALL_ZERO_TIMESTAMP.equals(value)) value = EPOCH_TIMESTAMP;
            String timestampFormat = timestampFormat(column.length());
            return LocalDateTime.from(DateTimeFormatter.ofPattern(timestampFormat).parse(value));
        case Types.TIMESTAMP_WITH_TIMEZONE:
            if (ALL_ZERO_TIMESTAMP.equals(value)) value = EPOCH_TIMESTAMP;
            return Timestamp.valueOf(value).toInstant().atZone(ZoneId.systemDefault());
        case Types.TIME:
            String timeFormat = timeFormat(column.length());
            return Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern(timeFormat).parse(value)));
        }
        return value;
    }

    private String timeFormat(int length) {
        String defaultFormat = "HH:mm:ss";
        if (length <= 0) return defaultFormat;
        StringBuilder format = new StringBuilder(defaultFormat);
        format.append(".");
        for (int i = 0; i < length; i++) {
            format.append("S");
        }
        return format.toString();
    }

    private String timestampFormat(int length) {
        String defaultFormat = "yyyy-MM-dd HH:mm:ss";
        if (length <= 0) return defaultFormat;
        StringBuilder format = new StringBuilder(defaultFormat);
        format.append(".");
        for (int i = 0; i < length; i++) {
            format.append("S");
        }
        return format.toString();
    }
}

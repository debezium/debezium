/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.math.BigDecimal;
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
            return convertToLocalDate(value);
        case Types.TIMESTAMP:
            return convertToLocalDateTime(column, value);
        case Types.TIMESTAMP_WITH_TIMEZONE:
            return convertToTimestamp(value);
        case Types.TIME:
            return convertToDuration(column, value);
        case Types.BOOLEAN:
            return convertToBoolean(value);
        case Types.BIT:
            return convertToBits(column, value);

        case Types.TINYINT:
        case Types.SMALLINT:
            return convertToSmallInt(value);

        case Types.NUMERIC:
        case Types.DECIMAL:
            return convertToDecimal(value);

        case Types.FLOAT:
        case Types.DOUBLE:
            return convertToDouble(value);
        case Types.BIGINT:
            return convertToBigInt(value);
        case Types.INTEGER:
            return convertToInteger(value);
        }
        return value;
    }

    /**
     * Converts a string object for an object type of {@link LocalDate}.
     * 0000-00-00 will be replaced with 1970-01-01;
     *
     * @param value the string object to be converted into a {@link LocalDate} type;
     * @return the converted value;
     */
    private Object convertToLocalDate(String value) {
        if (ALL_ZERO_DATE.equals(value)) value = EPOCH_DATE;
        return LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE.parse(value));
    }

    /**
     * Converts a string object for an object type of {@link LocalDateTime}.
     * 0000-00-00 00:00:00 will be replaced with 1970-01-01 00:00:00;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link LocalDateTime} type;
     * @return the converted value;
     */
    private Object convertToLocalDateTime(Column column, String value) {
        if (ALL_ZERO_TIMESTAMP.equals(value)) value = EPOCH_TIMESTAMP;
        String timestampFormat = timestampFormat(column.length());
        return LocalDateTime.from(DateTimeFormatter.ofPattern(timestampFormat).parse(value));
    }

    /**
     * Converts a string object for an object type of {@link Timestamp}.
     * 0000-00-00 00:00:00 will be replaced with 1970-01-01 00:00:00;
     *
     * @param value the string object to be converted into a {@link Timestamp} type;
     * @return the converted value;
     */
    private Object convertToTimestamp(String value) {
        if (ALL_ZERO_TIMESTAMP.equals(value)) value = EPOCH_TIMESTAMP;
        return Timestamp.valueOf(value).toInstant().atZone(ZoneId.systemDefault());
    }

    /**
     * Converts a string object for an object type of {@link Duration}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Duration} type;
     * @return the converted value;
     */
    private Object convertToDuration(Column column, String value) {
        String timeFormat = timeFormat(column.length());
        return Duration.between(LocalTime.MIN, LocalTime.from(DateTimeFormatter.ofPattern(timeFormat).parse(value)));
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param value the string object to be converted into a {@link Types#INTEGER} type;
     * @return the converted value;
     */
    private Object convertToInteger(String value) {
        return Integer.parseInt(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param value the string object to be converted into a {@link Types#INTEGER} type;
     * @return the converted value;
     */
    private Object convertToBigInt(String value) {
        return Long.valueOf(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param value the string object to be converted into a {@link Types#DOUBLE} type;
     * @return the converted value;
     */
    private Object convertToDouble(String value) {
        return Double.parseDouble(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DECIMAL}.
     *
     * @param value the string object to be converted into a {@link Types#DECIMAL} type;
     * @return the converted value;
     */
    private Object convertToDecimal(String value) {
        return new BigDecimal(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BIT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Types#BIT} type;
     * @return the converted value;
     */
    private Object convertToBits(Column column, String value) {
        if (column.length() > 1) {
            return convertToBits(value);
        }
        return convertToBit(value);
    }

    private Object convertToBit(String value) {
        try {
            return Short.parseShort(value) != 0;
        } catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private Object convertToBits(String value) {
        int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
        byte[] bytes = new byte[nums];
        for (int i = 0; i < nums; i++) {
            int s = value.length() - Byte.SIZE < 0 ? 0 : value.length() - Byte.SIZE;
            int e = value.length();
            bytes[nums - i - 1] = Byte.parseByte(value.substring(s, e), 2);
            value = value.substring(0, s);
        }
        return bytes;
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#SMALLINT}.
     *
     * @param value the string object to be converted into a {@link Types#SMALLINT} type;
     * @return the converted value;
     */
    private Object convertToSmallInt(String value) {
        return Short.parseShort(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BOOLEAN}.
     * @param value the string object to be converted into a {@link Types#BOOLEAN} type;
     *
     * @return the converted value;
     */
    private Object convertToBoolean(String value) {
        try {
            return Integer.parseInt(value) != 0;
        } catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
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

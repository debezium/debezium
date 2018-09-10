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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Pattern;

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

    private static final Pattern ALL_ZERO_TIMESTAMP = Pattern.compile("0000-00-00 00:00:00(\\.\\d{1,6})?");

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
            return convertToLocalDate(column, value);
        case Types.TIMESTAMP:
            return convertToLocalDateTime(column, value);
        case Types.TIMESTAMP_WITH_TIMEZONE:
            return convertToTimestamp(column, value);
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
        case Types.REAL:
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
     * If the column definition allows null and default value is 0000-00-00, we need return null;
     * else 0000-00-00 will be replaced with 1970-01-01;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link LocalDate} type;
     * @return the converted value;
     */
    private Object convertToLocalDate(Column column, String value) {
        final boolean zero = ALL_ZERO_DATE.equals(value) || "0".equals(value);
        if (zero && column.isOptional()) {
            return null;
        }
        if (zero) {
            value = EPOCH_DATE;
        }
        return LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE.parse(value));
    }

    /**
     * Converts a string object for an object type of {@link LocalDateTime}.
     * If the column definition allows null and default value is 0000-00-00 00:00:00, we need return null,
     * else 0000-00-00 00:00:00 will be replaced with 1970-01-01 00:00:00;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link LocalDateTime} type;
     * @return the converted value;
     */
    private Object convertToLocalDateTime(Column column, String value) {
        final boolean matches = ALL_ZERO_TIMESTAMP.matcher(value).matches() || "0".equals(value);
        if (matches) {
            if (column.isOptional()) {
                return null;
            }

            value = EPOCH_TIMESTAMP;
        }

        return LocalDateTime.from(timestampFormat(column.length()).parse(value));
    }

    /**
     * Converts a string object for an object type of {@link Timestamp}.
     * If the column definition allows null and default value is 0000-00-00 00:00:00, we need return null,
     * else 0000-00-00 00:00:00 will be replaced with 1970-01-01 00:00:00;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Timestamp} type;
     * @return the converted value;
     */
    private Object convertToTimestamp(Column column, String value) {
        final boolean matches = ALL_ZERO_TIMESTAMP.matcher(value).matches() || "0".equals(value) || EPOCH_TIMESTAMP.equals(value);
        if (matches) {
            if (column.isOptional()) {
                return null;
            }

            return Timestamp.from(Instant.EPOCH);
        }
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
        return Duration.between(LocalTime.MIN, LocalTime.from(timeFormat(column.length()).parse(value)));
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

    private DateTimeFormatter timeFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss");
        if (length !=-1) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }

    private DateTimeFormatter timestampFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss");
        if (length !=-1) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }
}

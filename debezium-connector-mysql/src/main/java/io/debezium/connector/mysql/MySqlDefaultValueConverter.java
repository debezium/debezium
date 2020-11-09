/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.ValueConverter;

/**
 * This class is used by a DDL parser to convert the string default value to a Java type
 * recognized by value converters for a subset of types. The functionality is kept separate
 * from the main converters to centralize the formatting logic if necessary.
 *
 * @author Jiri Pechanec
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public class MySqlDefaultValueConverter {

    private static final Pattern EPOCH_EQUIVALENT_TIMESTAMP = Pattern.compile("(\\d{4}-\\d{2}-00|\\d{4}-00-\\d{2}|0000-\\d{2}-\\d{2}) (00:00:00(\\.\\d{1,6})?)");

    private static final Pattern EPOCH_EQUIVALENT_DATE = Pattern.compile("\\d{4}-\\d{2}-00|\\d{4}-00-\\d{2}|0000-\\d{2}-\\d{2}");

    private static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:00";

    private static final String EPOCH_DATE = "1970-01-01";

    private final MySqlValueConverters converters;

    public MySqlDefaultValueConverter(MySqlValueConverters converters) {
        this.converters = converters;
    }

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

        // boolean is also INT(1) or TINYINT(1)
        if ("TINYINT".equals(column.typeName()) || "INT".equals(column.typeName())) {
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return convertToBoolean(value);
            }
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

            case Types.NUMERIC:
            case Types.DECIMAL:
                return convertToDecimal(column, value);

            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return convertToDouble(value);
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
        final boolean zero = EPOCH_EQUIVALENT_DATE.matcher(value).matches() || "0".equals(value);
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
        final boolean matches = EPOCH_EQUIVALENT_TIMESTAMP.matcher(value).matches() || "0".equals(value);
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
        final boolean matches = EPOCH_EQUIVALENT_TIMESTAMP.matcher(value).matches() || "0".equals(value) || EPOCH_TIMESTAMP.equals(value);
        if (matches) {
            if (column.isOptional()) {
                return null;
            }

            return Timestamp.from(Instant.EPOCH);
        }
        value = cleanTimestamp(value);
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
        return MySqlValueConverters.stringToDuration(value);
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
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link Types#DECIMAL} type;
     * @return the converted value;
     */
    private Object convertToDecimal(Column column, String value) {
        return column.scale().isPresent() ? new BigDecimal(value).setScale(column.scale().get(), RoundingMode.HALF_UP) : new BigDecimal(value);
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
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private Object convertToBits(String value) {
        int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
        byte[] bytes = new byte[nums];
        for (int i = 0; i < nums; i++) {
            int s = value.length() - Byte.SIZE < 0 ? 0 : value.length() - Byte.SIZE;
            int e = value.length();
            bytes[nums - i - 1] = (byte) Integer.parseInt(value.substring(s, e), 2);
            value = value.substring(0, s);
        }
        return bytes;
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
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private DateTimeFormatter timestampFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd")
                .optionalStart()
                .appendLiteral(" ")
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .optionalEnd()
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
        if (length > 0) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }

    /**
     * Clean input timestamp to yyyy-mm-dd hh:mm:ss[.fffffffff] format
     *
     * @param s input timestamp
     * @return cleaned timestamp
     */
    private String cleanTimestamp(String s) {
        if (s == null) {
            throw new java.lang.IllegalArgumentException("null string");
        }

        s = s.trim();

        final int MAX_MONTH = 12;
        final int MAX_DAY = 31;

        // Parse the date
        int firstDash = s.indexOf('-');
        int secondDash = s.indexOf('-', firstDash + 1);

        int dividingStart = -1;
        int dividingEnd = -1;
        boolean hasDividing = false;
        for (int i = secondDash + 1; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                dividingStart = i;
                hasDividing = true;
                break;
            }
        }
        if (hasDividing) {
            for (int i = dividingStart; i < s.length(); i++) {
                if (Character.isDigit(s.charAt(i))) {
                    break;
                }
                dividingEnd = i;
            }
        }

        // Parse the time
        int firstColon = s.indexOf(':', dividingEnd + 1);
        int secondColon = s.indexOf(':', firstColon + 1);
        int period = s.indexOf('.', secondColon + 1);

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;

        // Get the date
        int len = s.length();
        boolean parsedDate = false;
        if (firstDash > 0 && secondDash > firstDash) {
            year = Integer.parseInt(s.substring(0, firstDash));
            month = Integer.parseInt(s.substring(firstDash + 1, secondDash));
            if (hasDividing) {
                day = Integer.parseInt(s.substring(secondDash + 1, dividingStart));
            }
            else {
                day = Integer.parseInt(s.substring(secondDash + 1, len));
            }

            if ((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY)) {
                parsedDate = true;
            }
        }
        if (!parsedDate) {
            throw new java.lang.IllegalArgumentException("Cannot parse the date from " + s);
        }

        // Get the time. Hour, minute, second and colons are all optional
        if (hasDividing && dividingEnd < len - 1) {
            if (firstColon == -1) {
                hour = Integer.parseInt(s.substring(dividingEnd + 1, len));
            }
            else {
                hour = Integer.parseInt(s.substring(dividingEnd + 1, firstColon));
                if (firstColon < len - 1) {
                    if (secondColon == -1) {
                        minute = Integer.parseInt(s.substring(firstColon + 1, len));
                    }
                    else {
                        minute = Integer.parseInt(s.substring(firstColon + 1, secondColon));
                        if (secondColon < len - 1) {
                            if (period == -1) {
                                second = Integer.parseInt(s.substring(secondColon + 1, len));
                            }
                            else {
                                second = Integer.parseInt(s.substring(secondColon + 1, period));
                            }
                        }
                    }
                }
            }
        }

        StringBuilder cleanedTimestamp = new StringBuilder();
        cleanedTimestamp = cleanedTimestamp
                .append(String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second));

        if (period != -1 && period < len - 1) {
            cleanedTimestamp = cleanedTimestamp.append(".").append(s.substring(period + 1));
        }

        return cleanedTimestamp.toString();
    }

    public ColumnEditor setColumnDefaultValue(ColumnEditor columnEditor) {
        final Column column = columnEditor.create();

        // if converters is not null and the default value is not null, we need to convert default value
        if (converters != null && columnEditor.defaultValue() != null) {
            Object defaultValue = columnEditor.defaultValue();
            final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
            if (schemaBuilder == null) {
                return columnEditor;
            }
            final Schema schema = schemaBuilder.build();
            // In order to get the valueConverter for this column, we have to create a field;
            // The index value -1 in the field will never used when converting default value;
            // So we can set any number here;
            final Field field = new Field(columnEditor.name(), -1, schema);
            final ValueConverter valueConverter = converters.converter(columnEditor.create(), field);
            if (defaultValue instanceof String) {
                defaultValue = convert(column, (String) defaultValue);
            }
            defaultValue = valueConverter.convert(defaultValue);
            columnEditor.defaultValue(defaultValue);
        }
        return columnEditor;
    }
}

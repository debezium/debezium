/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

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
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Collect;

/**
 * Used by binlog-based connector DDL parsers to convert string default values to a specific Java type
 * recognized by value converters for a subset of types. The functionality is kept separate from the
 * main converters to centralize the formatting logic, if necessary.
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 * @see com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
 */
@Immutable
public abstract class BinlogDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogDefaultValueConverter.class);

    private static final Pattern EPOCH_EQUIVALENT_TIMESTAMP = Pattern.compile("(\\d{4}-\\d{2}-00|\\d{4}-00-\\d{2}|0000-\\d{2}-\\d{2}) (00:00:00(\\.\\d{1,6})?)");
    private static final Pattern EPOCH_EQUIVALENT_DATE = Pattern.compile("\\d{4}-\\d{2}-00|\\d{4}-00-\\d{2}|0000-\\d{2}-\\d{2}");
    private static final String EPOCH_TIMESTAMP = "1970-01-01 00:00:00";
    private static final String EPOCH_DATE = "1970-01-01";
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("([0-9]*-[0-9]*-[0-9]*) ([0-9]*:[0-9]*:[0-9]*(\\.([0-9]*))?)");
    private static final Pattern CHARSET_INTRODUCER_PATTERN = Pattern.compile("^_[A-Za-z0-9]+'(.*)'$");

    // Default values of these data types and number data types need to be trimmed.
    @Immutable
    private static final Set<Integer> TRIM_DATA_TYPES_BESIDES_NUMBER = Collect.unmodifiableSet(Types.DATE,
            Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME, Types.BOOLEAN);

    @Immutable
    private static final Set<Integer> NUMBER_DATA_TYPES = Collect.unmodifiableSet(Types.BIT, Types.TINYINT,
            Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC,
            Types.DECIMAL);

    private static final DateTimeFormatter ISO_LOCAL_DATE_WITH_OPTIONAL_TIME = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .optionalEnd()
            .toFormatter();

    private final BinlogValueConverters converters;

    public BinlogDefaultValueConverter(BinlogValueConverters converters) {
        this.converters = converters;
    }

    /**
     * This interface is used by a DDL parser to convert the string default value to a Java type
     * recognized by value converters for a subset of types.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param defaultValueExpression the default value literal; may be null
     * @return value converted to a Java type; optional
     */
    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        final Object logicalDefaultValue = convert(column, defaultValueExpression);
        if (logicalDefaultValue == null) {
            return Optional.empty();
        }

        final SchemaBuilder schemaBuilder = converters.schemaBuilder(column);
        if (schemaBuilder == null) {
            return Optional.of(logicalDefaultValue);
        }

        // In order to get the valueConverter for this column, we have to create a field;
        // The index value -1 in the field will never used when converting default value;
        // So we can set any number here;
        final Field field = new Field(column.name(), -1, schemaBuilder.build());
        final ValueConverter valueConverter = converters.converter(column, field);
        return Optional.ofNullable(valueConverter.convert(logicalDefaultValue));
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

        // trim non varchar data types before converting
        int jdbcType = column.jdbcType();
        if (TRIM_DATA_TYPES_BESIDES_NUMBER.contains(jdbcType) || NUMBER_DATA_TYPES.contains(jdbcType)) {
            value = value.trim();
        }

        // strip character set introducer on default value expressions
        value = stripCharacterSetIntroducer(value);

        // boolean is also INT(1) or TINYINT(1)
        if (NUMBER_DATA_TYPES.contains(jdbcType) && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) {
            if (Types.DECIMAL == jdbcType || Types.NUMERIC == jdbcType) {
                return convertToDecimal(column, value.equalsIgnoreCase("true") ? "1" : "0");
            }
            return value.equalsIgnoreCase("true") ? 1 : 0;
        }
        switch (jdbcType) {
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
     * Converts a string object for an object type of {@link LocalDate} or {@link LocalDateTime} in case of
     * a data type. If the column definition allows null and default value is 0000-00-00, we need return null;
     * else 0000-00-00 will be replaced with 1970-01-01;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param value the string object to be converted into a {@link LocalDate} type or {@link LocalDateTime} in case of date type;
     * @return the converted value;
     */
    private Object convertToLocalDate(Column column, String value) {
        final boolean zero = EPOCH_EQUIVALENT_DATE.matcher(value).matches()
                || EPOCH_EQUIVALENT_TIMESTAMP.matcher(value).matches()
                || "0".equals(value);

        if (zero && column.isOptional()) {
            return null;
        }
        if (zero) {
            value = EPOCH_DATE;
        }

        try {
            return LocalDate.from(ISO_LOCAL_DATE_WITH_OPTIONAL_TIME.parse(value));
        }
        catch (Exception e) {
            LOGGER.warn("Invalid default value '{}' for date column '{}'; {}", value, column.name(), e.getMessage());
            if (column.isOptional()) {
                return null;
            }
            else {
                return LocalDate.from(ISO_LOCAL_DATE_WITH_OPTIONAL_TIME.parse(EPOCH_DATE));
            }
        }
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

        try {
            return LocalDateTime.from(timestampFormat(column.length()).parse(value));
        }
        catch (Exception e) {
            LOGGER.warn("Invalid default value '{}' for datetime column '{}'; {}", value, column.name(), e.getMessage());
            if (column.isOptional()) {
                return null;
            }
            else {
                return LocalDateTime.from(timestampFormat(column.length()).parse(EPOCH_TIMESTAMP));
            }
        }
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
        final boolean matches = EPOCH_EQUIVALENT_TIMESTAMP.matcher(value).matches()
                || "0".equals(value)
                || EPOCH_TIMESTAMP.equals(value);
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
        Matcher matcher = TIMESTAMP_PATTERN.matcher(value);
        if (matcher.matches()) {
            value = matcher.group(2);
        }
        return BinlogValueConverters.stringToDuration(value);
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
        return column.scale().isPresent()
                ? new BigDecimal(value).setScale(column.scale().get(), RoundingMode.HALF_UP)
                : new BigDecimal(value);
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
            int s = Math.max(value.length() - Byte.SIZE, 0);
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

        // clean first dash
        s = replaceFirstNonNumericSubstring(s, 0, '-');
        // clean second dash
        s = replaceFirstNonNumericSubstring(s, s.indexOf('-') + 1, '-');
        // clean dividing space
        s = replaceFirstNonNumericSubstring(s, s.indexOf('-', s.indexOf('-') + 1) + 1, ' ');
        if (s.indexOf(' ') != -1) {
            // clean first colon
            s = replaceFirstNonNumericSubstring(s, s.indexOf(' ') + 1, ':');
            if (s.indexOf(':') != -1) {
                // clean second colon
                s = replaceFirstNonNumericSubstring(s, s.indexOf(':') + 1, ':');
            }
        }

        final int MAX_MONTH = 12;
        final int MAX_DAY = 31;

        // Parse the date
        int firstDash = s.indexOf('-');
        int secondDash = s.indexOf('-', firstDash + 1);
        int dividingSpace = s.indexOf(' ');

        // Parse the time
        int firstColon = s.indexOf(':', dividingSpace + 1);
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
            if (dividingSpace != -1) {
                day = Integer.parseInt(s.substring(secondDash + 1, dividingSpace));
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
        if (dividingSpace != -1 && dividingSpace < len - 1) {
            if (firstColon == -1) {
                hour = Integer.parseInt(s.substring(dividingSpace + 1, len));
            }
            else {
                hour = Integer.parseInt(s.substring(dividingSpace + 1, firstColon));
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
        cleanedTimestamp.append(String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second));
        if (period != -1 && period < len - 1) {
            cleanedTimestamp.append(".").append(s.substring(period + 1));
        }

        return cleanedTimestamp.toString();
    }

    /**
     * Replace the first non-numeric substring
     *
     * @param s the original string
     * @param startIndex the beginning index, inclusive
     * @param c the new character
     * @return the mutated string
     */
    private String replaceFirstNonNumericSubstring(String s, int startIndex, char c) {
        StringBuilder sb = new StringBuilder();
        sb.append(s.substring(0, startIndex));

        String rest = s.substring(startIndex);
        sb.append(rest.replaceFirst("[^\\d]+", Character.toString(c)));
        return sb.toString();
    }

    private String stripCharacterSetIntroducer(String value) {
        final Matcher matcher = CHARSET_INTRODUCER_PATTERN.matcher(value);
        return !matcher.matches() ? value : matcher.group(1);
    }
}

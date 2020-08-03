package io.debezium.jdbc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.*;
import io.debezium.util.HexConverter;
import io.debezium.util.NumberConversions;
import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.TemporalAdjuster;
import java.util.Base64;
import java.util.BitSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.debezium.util.NumberConversions.*;

public class ConverterHelper {

    private static final Logger logger = LoggerFactory.getLogger(ProxyConverter.class);


    public static ValueConverter convertBits(Column column, Field fieldDefn, ByteOrder byteOrderOfBitType) {
        if (column.length() > 1) {
            int numBits = column.length();
            int numBytes = numBits / Byte.SIZE + (numBits % Byte.SIZE == 0 ? 0 : 1);
            return (data) -> convertBits(column, fieldDefn, data, numBytes, byteOrderOfBitType);
        }
        return (data) -> convertBit(column, fieldDefn, data);
    }



    /**
     * Converts the given value for the given column/field.
     *
     * @param column
     *            describing the {@code data} value; never null
     * @param fieldDefn
     *            the field definition; never null
     * @param data
     *            the data object to be converted into a {@link Date Kafka Connect date} type
     * @param schemaFallback
     *            value that will be applied in case the column is defined as NOT NULL without a default value, but we
     *            still received no value; may happen e.g. when enabling MySQL's non-strict mode
     * @value converter
     *            conversion routine that will be invoked in case the value is not null
     *
     * @return The converted value. Will be {@code null} if the inbound value was {@code null} and the column is
     *         optional. Will be the column's default value (converted to the corresponding KC type, if the inbound
     *         value was {@code null}, the column is non-optional and has a default value. Will be {@code fallback} if
     *         the inbound value was {@code null}, the column is non-optional and has no default value. Otherwise, it
     *         will be the value produced by {@code callback} and lastly the result returned by
     *         {@link #handleUnknownData(Column, Field, Object)}.
     */
    public static Object convertValue(Column column, Field fieldDefn, Object data, Object schemaFallback, final Optional<Object> convertedValue) {
        if (data == null) {
            if (column.isOptional()) {
                return null;
            }
            final Object schemaDefault = fieldDefn.schema().defaultValue();
            return schemaDefault != null ? schemaDefault : schemaFallback;
        }
        logger.trace("Value from data object: *** {} ***", data);
        logger.trace("converted value is: {}", convertedValue);
        return convertedValue.orElseGet(() -> handleUnknownData(column, fieldDefn, data));
    }


    public static Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        if (column.isOptional() || fieldDefn.schema().isOptional()) {
            Class<?> dataClass = data.getClass();
            if (logger.isWarnEnabled()) {
                logger.warn("Unexpected value for JDBC type {} and column {}: class={}", column.jdbcType(), column,
                        dataClass.isArray() ? dataClass.getSimpleName() : dataClass.getName()); // don't include value in case its
                // sensitive
            }
            return null;
        }
        throw new IllegalArgumentException("Unexpected value for JDBC type " + column.jdbcType() + " and column " + column +
                ": class=" + data.getClass()); // don't include value in case its sensitive
    }


    static byte[] toByteArray(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        return byteBuffer.array();
    }

    public static ByteBuffer toByteBuffer(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        return StandardCharsets.UTF_8.encode(charBuffer);
    }

    /**
     * Converts the given byte array value into a byte buffer as preferred by Kafka Connect. Specific connectors
     * can perform value adjustments based on the column definition, e.g. right-pad with 0x00 bytes in case of
     * fixed length BINARY in MySQL.
     */
    public static ByteBuffer toByteBuffer(byte[] data) {
        // Kafka Connect would support raw byte arrays, too, but byte buffers are recommended
        return ByteBuffer.wrap(data);
    }


    public static ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] padLittleEndian(int numBytes, byte[] data) {
        if (data.length < numBytes) {
            byte[] padded = new byte[numBytes];
            System.arraycopy(data, 0, padded, 0, data.length);
            for (int i = data.length; i != numBytes; ++i) {
                padded[i] = 0;
            }
            return padded;
        }
        return data;
    }


    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME_WITH_TIMEZONE}.
     * The <a href="http://www.oracle.com/technetwork/articles/java/jf14-date-time-2125367.html">standard ANSI to Java 8 type
     * mappings</a> specify that the preferred mapping (when using JDBC's {@link java.sql.ResultSet#getObject(int) getObject(...)}
     * methods) in Java 8 is to return {@link OffsetTime} for these values.
     * <p>
     * This method handles several types of objects, including {@link OffsetTime}, {@link java.sql.Time}, {@link java.util.Date},
     * {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types have date components, those date
     * components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimeWithZone(Column column, Field fieldDefn, JdbcValueConverters.ValueConverterConfiguration configuration, Object data) {
        return convertValue(column, fieldDefn, data, configuration.fallbackTimeWithTimeZone, Optional.ofNullable(ZonedTime.toIsoString(data, configuration.defaultOffset, configuration.adjuster)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP} to {@link Timestamp} values, or milliseconds
     * past epoch.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Timestamp} instances, which have date and time info
     * but no time zone info. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such
     * as {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        return convertValue(column, fieldDefn, data, 0L, Optional.of(Timestamp.toEpochMillis(data, adjuster)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP} to {@link MicroTimestamp} values, or
     * microseconds past epoch.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Timestamp} instances, which have date and time info
     * but no time zone info. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such
     * as {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, Optional.of(MicroTimestamp.toEpochMicros(data, adjuster)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP} to {@link NanoTimestamp} values, or
     * nanoseconds past epoch.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Timestamp} instances, which have date and time info
     * but no time zone info. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such
     * as {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    static Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, Optional.of(NanoTimestamp.toEpochNanos(data, adjuster)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP} to {@link java.util.Date} values representing
     * milliseconds past epoch.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Timestamp} instances, which have date and time info
     * but no time zone info. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such
     * as {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L), Optional.of(new java.util.Date(Timestamp.toEpochMillis(data, adjuster))));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME} to {@link Time} values, or milliseconds past
     * midnight.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances that have no notion of date or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types might
     * have date components, those date components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimeToMillisPastMidnight(Column column, Field fieldDefn, Object data, boolean supportsLargeTimeValues) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, Optional.of(Time.toMilliOfDay(data, supportsLargeTimeValues)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME} to {@link MicroTime} values, or microseconds past
     * midnight.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances that have no notion of date or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types might
     * have date components, those date components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimeToMicrosPastMidnight(Column column, Field fieldDefn, Object data, boolean supportsLargeTimeValues) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, Optional.of(MicroTime.toMicroOfDay(data, supportsLargeTimeValues)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME} to {@link NanoTime} values, or nanoseconds past
     * midnight.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances that have no notion of date or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types might
     * have date components, those date components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimeToNanosPastMidnight(Column column, Field fieldDefn, Object data, boolean supportsLargeTimeValues) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, Optional.of(NanoTime.toNanoOfDay(data, supportsLargeTimeValues)));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIME} to {@link java.util.Date} values representing
     * the milliseconds past midnight on the epoch day.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Time} instances that have no notion of date or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}. If any of the types might
     * have date components, those date components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimeToMillisPastMidnightAsDate(Column column, Field fieldDefn, Object data, boolean supportsLargeTimeValues) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L),
                Optional.of(new java.util.Date(Time.toMilliOfDay(data, supportsLargeTimeValues))));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DATE} to the number of days past epoch.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Date} instances that have no notion of time or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalDate}, and {@link java.time.LocalDateTime}. If any of the types might
     * have time components, those time components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertDateToEpochDays(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        int epochDate = 0;
        try {
            epochDate = Date.toEpochDay(data, adjuster);
        }
        catch (IllegalArgumentException e) {
            logger.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), data.getClass(), data);
        }
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0, Optional.of(epochDate));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DATE} to the number of days past epoch, but represented
     * as a {@link java.util.Date} value at midnight on the date.
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Date} instances that have no notion of time or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalDate}, and {@link java.time.LocalDateTime}. If any of the types might
     * have time components, those time components are ignored.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data, TemporalAdjuster adjuster) {
        // epoch is the fallback value
        try {
            int epochDay = Date.toEpochDay(data, adjuster);
            long epochMillis = TimeUnit.DAYS.toMillis(epochDay);
            return convertValue(column, fieldDefn, data, new java.util.Date(0L), Optional.of(new java.util.Date(epochMillis)));
        }
        catch (IllegalArgumentException e) {
            logger.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                    fieldDefn.schema(), data.getClass(), data);
        }
        return null; //???
    }


    /**
     * Converts a value object for an expected JDBC type of {@link Types#TIMESTAMP_WITH_TIMEZONE}.
     * The <a href="http://www.oracle.com/technetwork/articles/java/jf14-date-time-2125367.html">standard ANSI to Java 8 type
     * mappings</a> specify that the preferred mapping (when using JDBC's {@link java.sql.ResultSet#getObject(int) getObject(...)}
     * methods) in Java 8 is to return {@link OffsetDateTime} for these values.
     * <p>
     * This method handles several types of objects, including {@link OffsetDateTime}, {@link java.sql.Timestamp},
     * {@link java.util.Date}, {@link java.time.LocalTime}, and {@link java.time.LocalDateTime}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTimestampWithZone(Column column, Field fieldDefn, JdbcValueConverters.ValueConverterConfiguration conf, Object data) {
        return convertValue(column, fieldDefn, data, conf.fallbackTimestampWithTimeZone,
                Optional.ofNullable(ZonedTimestamp.toIsoString(data, conf.defaultOffset, conf.adjuster)));
    }

    public static Object convertBinary(Column column, Field fieldDefn, Object data, CommonConnectorConfig.BinaryHandlingMode mode) {
        switch (mode) {
            case BASE64:
                return convertBinaryToBase64(column, fieldDefn, data);
            case HEX:
                return convertBinaryToHex(column, fieldDefn, data);
            case BYTES:
            default:
                return convertBinaryToBytes(column, fieldDefn, data);
        }
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBinaryToBytes(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, BYTE_ZERO, Optional.ofNullable(binaryToBytes(fieldDefn, data)));
    }

    static Object binaryToBytes(Field fieldDefn, Object data) {
        if (data instanceof String) {
            return toByteBuffer(((String) data));
        }
        else if (data instanceof char[]) {
            return toByteBuffer((char[]) data);
        }
        else if (data instanceof byte[]) {
            return toByteBuffer((byte[]) data);
        }
        else {
            // An unexpected value
            return unexpectedBinary(data, fieldDefn);
        }
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBinaryToBase64(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", Optional.ofNullable((base64ToString(data, fieldDefn))));
        }

    static Object base64ToString(Object data, Field fieldDefn) {
        Base64.Encoder base64Encoder = Base64.getEncoder();

        if (data instanceof String) {
            return new String(base64Encoder.encode(((String) data).getBytes(StandardCharsets.UTF_8)));
        }
        else if (data instanceof char[]) {
            return new String(base64Encoder.encode(toByteArray((char[]) data)), StandardCharsets.UTF_8);
        }
        else if (data instanceof byte[]) {
            return new String(base64Encoder.encode((byte[]) data), StandardCharsets.UTF_8);
        }
        else {
            // An unexpected value
            return unexpectedBinary(data, fieldDefn);
        }
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBinaryToHex(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", binaryToHex(fieldDefn, data));
    }

    static Optional<Object> binaryToHex(Field fieldDefn, Object data) {
        Object out;
        if (data instanceof String) 
            out = HexConverter.convertToHexString(((String) data).getBytes(StandardCharsets.UTF_8));
        else if (data instanceof char[]) 
            out = HexConverter.convertToHexString(toByteArray((char[]) data));
        else if (data instanceof byte[]) 
            out = HexConverter.convertToHexString((byte[]) data);
        else out = unexpectedBinary(data, fieldDefn);
        return Optional.ofNullable(out);
    }


    /**
     * Handle the unexpected value from a row with a column type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param value the binary value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     * @see #convertBinaryToBytes(Column, Field, Object)
     */
    static byte[] unexpectedBinary(Object value, Field fieldDefn) {
        logger.warn("Unexpected JDBC BINARY value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TINYINT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertSmallInt(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#SMALLINT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, SHORT_FALSE, numsToShort(data));
    }

    static Optional<Object> numsToShort(Object data) {
        Object out;
        if (data instanceof Short) 
            out = data;
        else if (data instanceof Number) 
            out = ((Number) data).shortValue();
        else if (data instanceof Boolean) 
            out = NumberConversions.getShort((Boolean) data);
        else if (data instanceof String) 
            out = Short.valueOf((String) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertInteger(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0, numsToInt(data) );
    }

    static Optional<Object> numsToInt(Object data) {
        Object out;
        if (data instanceof Integer) 
            out = data;
        else if (data instanceof Number) 
            out = ((Number) data).intValue();
        else if (data instanceof Boolean) 
            out = NumberConversions.getInteger((Boolean) data);
        else if (data instanceof String) 
            out = Integer.valueOf((String) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBigInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, numsToLong(data));
    }

    static Optional<Object> numsToLong(Object data) {
        Object out;
        if (data instanceof Long) 
            out = data;
        else if (data instanceof Number) 
            out = ((Number) data).longValue();
        else if (data instanceof Boolean) 
            out = NumberConversions.getLong((Boolean) data);
        else if (data instanceof String) 
            out = Long.valueOf((String) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#FLOAT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertFloat(Column column, Field fieldDefn, Object data) {
        return convertDouble(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertDouble(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0.0d, numsToDouble(data));
    }

    static Optional<Object> numsToDouble(Object data) {
        Object out;
        if (data instanceof Double) 
            out = data;
        else if (data instanceof Number) 
            out = ((Number) data).doubleValue();
        else if (data instanceof SpecialValueDecimal)
            out = ((SpecialValueDecimal) data).toDouble();
        else if (data instanceof Boolean) 
            out = NumberConversions.getDouble((Boolean) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#REAL}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertReal(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0.0f, numsToReal(data));
    }

    static Optional<Object> numsToReal(Object data) {
        Object out;
        if (data instanceof Float) 
            out = data;
        else if (data instanceof Number) 
            out = ((Number) data).floatValue();
        else if (data instanceof Boolean) 
            out = NumberConversions.getFloat((Boolean) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertNumeric(Column column, Field fieldDefn, Object data, JdbcValueConverters.DecimalMode decimalMode) {
        return convertDecimal(column, fieldDefn, data, decimalMode);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertDecimal(Column column, Field fieldDefn, Object data, JdbcValueConverters.DecimalMode decimalMode) {
        if (data instanceof SpecialValueDecimal) {
            return SpecialValueDecimal.fromLogical((SpecialValueDecimal) data, decimalMode, column.name());
        }
        Object decimal = toBigDecimal(column, fieldDefn, data);
        if (decimal instanceof BigDecimal) {
            return SpecialValueDecimal.fromLogical(new SpecialValueDecimal((BigDecimal) decimal), decimalMode, column.name());
        }
        return decimal;
    }

    public static Object toBigDecimal(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, BigDecimal.ZERO, numsToBigDecimal(data));
    }

    static Optional<Object> numsToBigDecimal(Object data) {
        Object out;
        if (data instanceof BigDecimal) 
            out = data;
        else if (data instanceof Boolean) 
            out = NumberConversions.getBigDecimal((Boolean) data);
        else if (data instanceof Short) 
            out = new BigDecimal(((Short) data).intValue());
        else if (data instanceof Integer) 
            out = new BigDecimal((Integer) data);
        else if (data instanceof Long) 
            out = BigDecimal.valueOf((Long) data);
        else if (data instanceof Float) 
            out = BigDecimal.valueOf(((Float) data).doubleValue());
        else if (data instanceof Double) 
            out = BigDecimal.valueOf((Double) data);
        else if (data instanceof String) 
            out = new BigDecimal((String) data);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#CHAR}, {@link Types#VARCHAR},
     * {@link Types#LONGVARCHAR}, {@link Types#CLOB}, {@link Types#NCHAR}, {@link Types#NVARCHAR}, {@link Types#LONGNVARCHAR},
     * {@link Types#NCLOB}, {@link Types#DATALINK}, and {@link Types#SQLXML}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertString(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", objectToString(data, column));
    }

    private static Optional<Object> objectToString(Object data, Column column) {
        if (data instanceof SQLXML) {
            try {
                return  Optional.ofNullable(((SQLXML) data).getString());
            }
            catch (SQLException e) {
                throw new RuntimeException("Error processing data from " + column.jdbcType() + " and column " + column +
                        ": class=" + data.getClass(), e);
            }
        }
        else {
            return Optional.ofNullable(data.toString());
        }
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#ROWID}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} tr.deliver(ype; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertRowId(Column column, Field fieldDefn, Object data) {
        ByteBuffer rowid = null;
        if (data instanceof java.sql.RowId) {
            java.sql.RowId row = (java.sql.RowId) data;
            rowid = ByteBuffer.wrap(row.getBytes());
        }
        return convertValue(column, fieldDefn, data, BYTE_BUFFER_ZERO, Optional.ofNullable(rowid));
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BIT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBit(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, false, bitValues(data));
    }

    private static Optional<Object> bitValues(Object data) {
        Object out;
        if (data instanceof Boolean) 
            out = data;
        else if (data instanceof Short) 
            out = ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        else if (data instanceof Integer) 
            out = (Integer) data == 0 ? Boolean.FALSE : Boolean.TRUE;
        else if (data instanceof Long) 
            out = ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        else if (data instanceof BitSet) 
            out = ((BitSet) data).get(0);
        else out = null;
        return Optional.ofNullable(out);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BIT} of length 2+.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @param numBytes the number of bytes that should be included in the resulting byte[]
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBits(Column column, Field fieldDefn, Object data, int numBytes, ByteOrder byteOrderOfBitType) {
        return convertValue(column, fieldDefn, data, new byte[0], Optional.ofNullable(specialBitValues(data, numBytes, byteOrderOfBitType)));
    }

    static Object specialBitValues(Object data, int numBytes, ByteOrder byteOrderOfBitType) {
        if (data instanceof Boolean) {
            Boolean value = (Boolean) data;
            return new byte[]{value ? (byte) 1 : (byte) 0 };
        }
        else if (data instanceof Short) {
            Short value = (Short) data;
            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(value);
            return buffer.array();
        }
        else if (data instanceof Integer) {
            Integer value = (Integer) data;
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(value);
            return buffer.array();
        }
        else if (data instanceof Long) {
            Long value = (Long) data;
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(value);
            return buffer.array();
        }
        else if (data instanceof byte[]) {
            byte[] bytes = (byte[]) data;
            if (bytes.length == 1) {
                return bytes;
            }
            if (byteOrderOfBitType == ByteOrder.BIG_ENDIAN) {
                // Reverse it to little endian ...
                int i = 0;
                int j = bytes.length - 1;
                byte tmp;
                while (j > i) {
                    tmp = bytes[j];
                    bytes[j] = bytes[i];
                    bytes[i] = tmp;
                    ++i;
                    --j;
                }
            }
            return padLittleEndian(numBytes, bytes);
        }
        else if (data instanceof BitSet) {
            byte[] bytes = ((BitSet) data).toByteArray();
            return padLittleEndian(numBytes, bytes);
        }
        return null;
    }


    /**
     * Converts a value object for an expected JDBC type of {@link Types#BOOLEAN}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    public static Object convertBoolean(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, false, Optional.ofNullable(valuesToBoolean(data)));
    }

    private static Object valuesToBoolean(Object data) {
        if (data instanceof Boolean) {
            return data;
        }
        else if (data instanceof Short) {
            return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        else if (data instanceof Integer) {
            return (Integer) data == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        else if (data instanceof Long) {
            return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        }
        return null;
    }

    public static Object convertDate(Column column, Field field, JdbcValueConverters.ValueConverterConfiguration conf, Object data) {
        if (conf.adaptiveTimePrecisionMode || conf.adaptiveTimeMicrosecondsPrecisionMode) {
            return convertDateToEpochDays(column, field, data, conf.adjuster);
        }
        return convertDateToEpochDaysAsDate(column, field, data, conf.adjuster);
    }



    public static Object convertTime(Column column, Field fieldDefn, JdbcValueConverters.ValueConverterConfiguration conf, Object data) {
        if (conf.adaptiveTimeMicrosecondsPrecisionMode) {
            return convertTimeToMicrosPastMidnight(column, fieldDefn, data, conf.supportsLargeTimeValues);
        }
        if (conf.adaptiveTimePrecisionMode) {
            if (column.length() <= 3) {
                return convertTimeToMillisPastMidnight(column, fieldDefn, data, conf.supportsLargeTimeValues);
            }
            if (column.length() <= 6) {
                return convertTimeToMicrosPastMidnight(column, fieldDefn, data, conf.supportsLargeTimeValues);
            }
            return convertTimeToNanosPastMidnight(column, fieldDefn, data, conf.supportsLargeTimeValues);
        }
        // "connect" mode
        else {
            return convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data, conf.supportsLargeTimeValues);
        }
    }

    public static Object convertTimestamp(Column column, Field fieldDefn, JdbcValueConverters.ValueConverterConfiguration conf, Object data) {
        if (conf.adaptiveTimePrecisionMode || conf.adaptiveTimeMicrosecondsPrecisionMode) {
            if (conf.timeprecision.apply(column) <= 3) {
                return convertTimestampToEpochMillis(column, fieldDefn, data, conf.adjuster);
            }
            if (conf.timeprecision.apply(column) <= 6) {
                return convertTimestampToEpochMicros(column, fieldDefn, data, conf.adjuster);
            }
            return convertTimestampToEpochNanos(column, fieldDefn, data, conf.adjuster);
        }
        return convertTimestampToEpochMillisAsDate(column, fieldDefn, data, conf.adjuster);
    }
}
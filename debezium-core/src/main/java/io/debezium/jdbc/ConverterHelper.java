package io.debezium.jdbc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.time.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.TemporalAdjuster;
import java.util.Optional;

public final class ConverterHelper {

    private static Logger logger = LoggerFactory.getLogger(ProxyConverter.class);

    static ZoneOffset defaultOffset;

    /**
     * Fallback value for TIMESTAMP WITH TZ is epoch
     */
    static String fallbackTimestampWithTimeZone;

    /**
     * Fallback value for TIME WITH TZ is 00:00
     */
    static String fallbackTimeWithTimeZone;
    static boolean adaptiveTimePrecisionMode;
    static boolean adaptiveTimeMicrosecondsPrecisionMode;
    static JdbcValueConverters.DecimalMode decimalMode;
    static TemporalAdjuster adjuster;
    static JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode;
    static CommonConnectorConfig.BinaryHandlingMode binaryMode;
    static ByteOrder byteOrderOfBitType;


    public static void initiate(JdbcValueConverters.DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                           TemporalAdjuster adjuster, JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode, CommonConnectorConfig.BinaryHandlingMode binaryMode) {
        ConverterHelper.defaultOffset = defaultOffset != null ? defaultOffset : ZoneOffset.UTC;
        ConverterHelper.adaptiveTimePrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE);
        ConverterHelper.adaptiveTimeMicrosecondsPrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        ConverterHelper.decimalMode = decimalMode != null ? decimalMode : JdbcValueConverters.DecimalMode.PRECISE;
        ConverterHelper.adjuster = adjuster;
        ConverterHelper.bigIntUnsignedMode = bigIntUnsignedMode != null ? bigIntUnsignedMode : JdbcValueConverters.BigIntUnsignedMode.PRECISE;
        ConverterHelper.binaryMode = binaryMode != null ? binaryMode : CommonConnectorConfig.BinaryHandlingMode.BYTES;
        ConverterHelper.fallbackTimestampWithTimeZone = ZonedTimestamp.toIsoString(
                    OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT, defaultOffset),
            defaultOffset,
            adjuster);
        ConverterHelper.fallbackTimeWithTimeZone = ZonedTime.toIsoString(
                    OffsetTime.of(LocalTime.MIDNIGHT, defaultOffset),
            defaultOffset,
            adjuster);
    }

    public static boolean supportsLargeTimeValues() {
        return ConverterHelper.adaptiveTimePrecisionMode || ConverterHelper.adaptiveTimeMicrosecondsPrecisionMode;
    }


    static SchemaBuilder timestampBuilder(Column column) {
        if (ConverterHelper.supportsLargeTimeValues()) {
            if (column.length() <= 3) {
                return Timestamp.builder();
            }
            if (column.length() <= 6) {
                return MicroTimestamp.builder();
            }
            return NanoTimestamp.builder();
        }
        return org.apache.kafka.connect.data.Timestamp.builder();
    }



    static SchemaBuilder timeBuilder(Column column) {
        if (ConverterHelper.adaptiveTimeMicrosecondsPrecisionMode) {
            return MicroTime.builder();
        }
        if (ConverterHelper.adaptiveTimePrecisionMode) {
            if (column.length() <= 3) {
                return Time.builder();
            }
            if (column.length() <= 6) {
                return MicroTime.builder();
            }
            return NanoTime.builder();
        }
        return org.apache.kafka.connect.data.Time.builder();
    }

    static SchemaBuilder dateBuilder(Column column) {
        if (supportsLargeTimeValues()) { //TODO
            return Date.builder();
        }
        return org.apache.kafka.connect.data.Date.builder();
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
    static Object convertValue(Column column, Field fieldDefn, Object data, Object schemaFallback, final Optional<Object> convertedValue) {
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


    static Object handleUnknownData(Column column, Field fieldDefn, Object data) {
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

    static ByteBuffer toByteBuffer(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        return StandardCharsets.UTF_8.encode(charBuffer);
    }

    /**
     * Converts the given byte array value into a byte buffer as preferred by Kafka Connect. Specific connectors
     * can perform value adjustments based on the column definition, e.g. right-pad with 0x00 bytes in case of
     * fixed length BINARY in MySQL.
     */
    static ByteBuffer toByteBuffer(byte[] data) {
        // Kafka Connect would support raw byte arrays, too, but byte buffers are recommended
        return ByteBuffer.wrap(data);
    }


    static ByteBuffer toByteBuffer(String string) {
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

}

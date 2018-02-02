/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static io.debezium.util.NumberConversions.BYTE_ZERO;
import static io.debezium.util.NumberConversions.SHORT_FALSE;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.data.Bits;
import io.debezium.data.Xml;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various column types. This implementation is aware
 * of the most common JDBC types and values. Specializations for specific DBMSes can be addressed in subclasses.
 * <p>
 * Although it is more likely that values will correspond pretty closely to the expected JDBC types, this class assumes it is
 * possible for some variation to occur when values originate in libraries that are not JDBC drivers. Specifically, the conversion
 * logic for JDBC temporal types with timezones (e.g., {@link Types#TIMESTAMP_WITH_TIMEZONE}) do support converting values that
 * don't have timezones (e.g., {@link java.sql.Timestamp}) by assuming a default time zone offset for values that don't have
 * (but are expected to have) timezones. Again, when the values are highly-correlated with the expected SQL/JDBC types, this
 * default timezone offset will not be needed.
 *
 * @author Randall Hauch
 */
@Immutable
public class JdbcValueConverters implements ValueConverterProvider {

    public enum DecimalMode {
        PRECISE, DOUBLE;
    }

    public enum BigIntUnsignedMode {
        PRECISE, LONG;
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final ZoneOffset defaultOffset;
    protected final boolean adaptiveTimePrecisionMode;
    protected final boolean adaptiveTimeMicrosecondsPrecisionMode;
    protected final DecimalMode decimalMode;
    private final TemporalAdjuster adjuster;
    protected final BigIntUnsignedMode bigIntUnsignedMode;

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones, and uses adapts time and timestamp values based upon the precision of the database
     * columns.
     */
    public JdbcValueConverters() {
        this(null, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values without timezone
     * information to values that require timezones. This default offset should not be needed when values are highly-correlated
     * with the expected SQL/JDBC types.
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *            {@link DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param defaultOffset the zone offset that is to be used when converting non-timezone related values to values that do
     *            have timezones; may be null if UTC is to be used
     * @param adjuster the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     *            adjustment is necessary
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *            {@link BigIntUnsignedMode#PRECISE} is to be used
     */
    public JdbcValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                               TemporalAdjuster adjuster, BigIntUnsignedMode bigIntUnsignedMode) {
        this.defaultOffset = defaultOffset != null ? defaultOffset : ZoneOffset.UTC;
        this.adaptiveTimePrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE);
        this.adaptiveTimeMicrosecondsPrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        this.decimalMode = decimalMode != null ? decimalMode : DecimalMode.PRECISE;
        this.adjuster = adjuster;
        this.bigIntUnsignedMode = bigIntUnsignedMode != null ? bigIntUnsignedMode : BigIntUnsignedMode.PRECISE;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.NULL:
                logger.warn("Unexpected JDBC type: NULL");
                return null;

            // Single- and multi-bit values ...
            case Types.BIT:
                if (column.length() > 1) {
                    return Bits.builder(column.length());
                }
                // otherwise, it is just one bit so use a boolean ...
            case Types.BOOLEAN:
                return SchemaBuilder.bool();

            // Fixed-length binary values ...
            case Types.BLOB:
            case Types.BINARY:
                return SchemaBuilder.bytes();

            // Variable-length binary values ...
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return SchemaBuilder.bytes();

            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255
                return SchemaBuilder.int8();
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                return SchemaBuilder.int16();
            case Types.INTEGER:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                return SchemaBuilder.int32();
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                return SchemaBuilder.int64();

            // Numeric decimal numbers
            case Types.REAL:
                // values are single precision floating point number which supports 7 digits of mantissa.
                return SchemaBuilder.float32();
            case Types.FLOAT:
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                return SchemaBuilder.float64();
            case Types.NUMERIC:
            case Types.DECIMAL:
                switch (decimalMode) {
                    case DOUBLE:
                        return SchemaBuilder.float64();
                    case PRECISE:
                        // values are fixed-precision decimal values with exact precision.
                        // Use Kafka Connect's arbitrary precision decimal type and use the column's specified scale ...
                        return Decimal.builder(column.scale());
                }

                // Fixed-length string values
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
                return SchemaBuilder.string();

            // Variable-length string values
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.DATALINK:
                return SchemaBuilder.string();
            case Types.SQLXML:
                return Xml.builder();
            // Date and time values
            case Types.DATE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return Date.builder();
                }
                return org.apache.kafka.connect.data.Date.builder();
            case Types.TIME:
                if(adaptiveTimeMicrosecondsPrecisionMode) {
                    return MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (column.length() <= 3) return Time.builder();
                    if (column.length() <= 6) return MicroTime.builder();
                    return NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (column.length() <= 3) return Timestamp.builder();
                    if (column.length() <= 6) return MicroTimestamp.builder();
                    return NanoTimestamp.builder();
                }
                return org.apache.kafka.connect.data.Timestamp.builder();
            case Types.TIME_WITH_TIMEZONE:
                return ZonedTime.builder();
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return ZonedTimestamp.builder();

            // Other types ...
            case Types.ROWID:
                // often treated as a string, but we'll generalize and treat it as a byte array
                return SchemaBuilder.bytes();

            // Unhandled types
            case Types.ARRAY:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                break;
        }
        return null;

    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.NULL:
                return (data) -> null;
            case Types.BIT:
                return convertBits(column, fieldDefn);
            case Types.BOOLEAN:
                return (data) -> convertBoolean(column, fieldDefn, data);

            // Binary values ...
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return (data) -> convertBinary(column, fieldDefn, data);

            // Numeric integers
            case Types.TINYINT:
                return (data) -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                return (data) -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                return (data) -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                return (data) -> convertBigInt(column, fieldDefn, data);

            // Numeric decimal numbers
            case Types.FLOAT:
                return (data) -> convertFloat(column, fieldDefn, data);
            case Types.DOUBLE:
                return (data) -> convertDouble(column, fieldDefn, data);
            case Types.REAL:
                return (data) -> convertReal(column, fieldDefn, data);
            case Types.NUMERIC:
                switch (decimalMode) {
                    case DOUBLE:
                        return (data) -> convertDouble(column, fieldDefn, data);
                    case PRECISE:
                        return (data) -> convertNumeric(column, fieldDefn, data);
                }
            case Types.DECIMAL:
                switch (decimalMode) {
                    case DOUBLE:
                        return (data) -> convertDouble(column, fieldDefn, data);
                    case PRECISE:
                        return (data) -> convertDecimal(column, fieldDefn, data);
                }

                // String values
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
            case Types.SQLXML:
                return (data) -> convertString(column, fieldDefn, data);

            // Date and time values
            case Types.DATE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return (data) -> convertDateToEpochDays(column, fieldDefn, data);
                }
                return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data);
            case Types.TIME:
                if(adaptiveTimeMicrosecondsPrecisionMode) {
                    return data -> convertTimeToMicrosPastMidnight(column, fieldDefn, data);
                }
                if (adaptiveTimePrecisionMode) {
                    if (column.length() <= 3) return data -> convertTimeToMillisPastMidnight(column, fieldDefn, data);
                    if (column.length() <= 6) return data -> convertTimeToMicrosPastMidnight(column, fieldDefn, data);
                    return (data) -> convertTimeToNanosPastMidnight(column, fieldDefn, data);
                }
                return (data) -> convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data);
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (column.length() <= 3) return data -> convertTimestampToEpochMillis(column, fieldDefn, data);
                    if (column.length() <= 6) return data -> convertTimestampToEpochMicros(column, fieldDefn, data);
                    return (data) -> convertTimestampToEpochNanos(column, fieldDefn, data);
                }
                return (data) -> convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
            case Types.TIME_WITH_TIMEZONE:
                return (data) -> convertTimeWithZone(column, fieldDefn, data);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);

            // Other types ...
            case Types.ROWID:
                return (data) -> convertRowId(column, fieldDefn, data);

            // Unhandled types
            case Types.ARRAY:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                return null;
        }
    }

    protected ValueConverter convertBits(Column column, Field fieldDefn) {
        if (column.length() > 1) {
            int numBits = column.length();
            int numBytes = numBits / Byte.SIZE + (numBits % Byte.SIZE == 0 ? 0 : 1);
            return (data) -> convertBits(column, fieldDefn, data, numBytes);
        }
        return (data) -> convertBit(column, fieldDefn, data);
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
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            data = OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT, defaultOffset); // return epoch
        }
        try {
            return ZonedTimestamp.toIsoString(data, defaultOffset, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            data = OffsetTime.of(LocalTime.MIDNIGHT, defaultOffset); // return epoch time
        }
        try {
            return ZonedTime.toIsoString(data, defaultOffset, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return Timestamp.toEpochMillis(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return MicroTimestamp.toEpochMicros(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return NanoTimestamp.toEpochNanos(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return new java.util.Date(0L); // return epoch
        }
        try {
            return new java.util.Date(Timestamp.toEpochMillis(data, adjuster));
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimeToMillisPastMidnight(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0; // return epoch
        }
        try {
            return Time.toMilliOfDay(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimeToMicrosPastMidnight(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return MicroTime.toMicroOfDay(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimeToNanosPastMidnight(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return NanoTime.toNanoOfDay(data, adjuster);
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertTimeToMillisPastMidnightAsDate(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L; // return epoch
        }
        try {
            return new java.util.Date(Time.toMilliOfDay(data, adjuster));
        } catch (IllegalArgumentException e) {
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0; // return epoch
        }
        try {
            return Date.toEpochDay(data, adjuster);
        } catch (IllegalArgumentException e) {
            logger.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                        fieldDefn.schema(), data.getClass(), data);
            return handleUnknownData(column, fieldDefn, data);
        }
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
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return new java.util.Date(0L); // return epoch
        }
        try {
            int epochDay = Date.toEpochDay(data, adjuster);
            long epochMillis = TimeUnit.DAYS.toMillis(epochDay);
            return new java.util.Date(epochMillis);
        } catch (IllegalArgumentException e) {
            logger.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                        fieldDefn.schema(), data.getClass(), data);
            return null;
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
    protected Object convertBinary(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            data = BYTE_ZERO;
        }
        if (data instanceof char[]) {
            data = new String((char[]) data); // convert to string
        }
        if (data instanceof String) {
            // This was encoded as a hexadecimal string, but we receive it as a normal string ...
            data = ((String) data).getBytes();
        }
        if (data instanceof byte[]) {
            return convertByteArray(column, (byte[]) data);
        }

        // An unexpected value
        return unexpectedBinary(data, fieldDefn);
    }

    /**
     * Converts the given byte array value into a byte buffer as preferred by Kafka Connect. Specific connectors
     * can perform value adjustments based on the column definition, e.g. right-pad with 0x00 bytes in case of
     * fixed length BINARY in MySQL.
     */
    protected ByteBuffer convertByteArray(Column column, byte[] data) {
        // Kafka Connect would support raw byte arrays, too, but byte buffers are recommended
        return ByteBuffer.wrap(data);
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param value the binary value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     * @see #convertBinary(Column, Field, Object)
     */
    protected byte[] unexpectedBinary(Object value, Field fieldDefn) {
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
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
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
    protected Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return SHORT_FALSE;
        }
        if (data instanceof Short) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Short(value.shortValue());
        }
        if (data instanceof Boolean) {
            return NumberConversions.getShort((Boolean) data);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0;
        }
        if (data instanceof Integer) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return Integer.valueOf(value.intValue());
        }
        if (data instanceof Boolean) {
            return NumberConversions.getInteger((Boolean) data);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0L;
        }
        if (data instanceof Long) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return Long.valueOf(value.longValue());
        }
        if (data instanceof Boolean) {
            return NumberConversions.getLong((Boolean) data);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
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
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0.0d;
        }
        if (data instanceof Double) return data;
        if (data instanceof Number) {
            // Includes BigDecimal and other numeric values ...
            Number value = (Number) data;
            return Double.valueOf(value.doubleValue());
        }
        if (data instanceof Boolean) {
            return NumberConversions.getDouble((Boolean) data);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return 0.0f;
        }
        if (data instanceof Float) return data;
        if (data instanceof Number) {
            // Includes BigDecimal and other numeric values ...
            Number value = (Number) data;
            return Float.valueOf(value.floatValue());
        }
        if (data instanceof Boolean) {
            return NumberConversions.getFloat((Boolean) data);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) {
                return null;
            }
            else {
                return BigDecimal.ZERO;
            }
        }
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = NumberConversions.getBigDecimal((Boolean) data);
        else if (data instanceof Short)
            decimal = new BigDecimal(((Short) data).intValue());
        else if (data instanceof Integer)
            decimal = new BigDecimal(((Integer) data).intValue());
        else if (data instanceof Long)
            decimal = BigDecimal.valueOf(((Long) data).longValue());
        else if (data instanceof Float)
            decimal = BigDecimal.valueOf(((Float) data).doubleValue());
        else if (data instanceof Double)
            decimal = BigDecimal.valueOf(((Double) data).doubleValue());
        else {
            return handleUnknownData(column, fieldDefn, data);
        }
        return decimal;
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
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) {
                return null;
            }
            else {
                return BigDecimal.ZERO;
            }
        }
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = NumberConversions.getBigDecimal((Boolean) data);
        else if (data instanceof Short)
            decimal = new BigDecimal(((Short) data).intValue());
        else if (data instanceof Integer)
            decimal = new BigDecimal(((Integer) data).intValue());
        else if (data instanceof Long)
            decimal = BigDecimal.valueOf(((Long) data).longValue());
        else if (data instanceof Float)
            decimal = BigDecimal.valueOf(((Float) data).doubleValue());
        else if (data instanceof Double)
            decimal = BigDecimal.valueOf(((Double) data).doubleValue());
        else {
            return handleUnknownData(column, fieldDefn, data);
        }
        return decimal;
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
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return "";
        }
        if (data instanceof SQLXML) {
            try {
                return ((SQLXML) data).getString();
            } catch (SQLException e) {
                throw new RuntimeException("Error processing data from " + column.jdbcType() + " and column " + column +
                        ": class=" + data.getClass(), e);
            }
        }
        return data.toString();
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#ROWID}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertRowId(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return ByteBuffer.wrap(new byte[0]);
        }
        if (data instanceof java.sql.RowId) {
            java.sql.RowId row = (java.sql.RowId) data;
            return ByteBuffer.wrap(row.getBytes());
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return false;
        }
        if (data instanceof Boolean) return data;
        if (data instanceof Short) return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Integer) return ((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Long) return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof BitSet) {
            BitSet value = (BitSet) data;
            return value.get(0);
        }
        return handleUnknownData(column, fieldDefn, data);
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
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return false;
        }
        if (data instanceof Boolean) {
            Boolean value = (Boolean) data;
            return new byte[] { value.booleanValue() ? (byte) 1 : (byte) 0 };
        }
        if (data instanceof Short) {
            Short value = (Short) data;
            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(value.shortValue());
            return buffer.array();
        }
        if (data instanceof Integer) {
            Integer value = (Integer) data;
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(value.intValue());
            return buffer.array();
        }
        if (data instanceof Long) {
            Long value = (Long) data;
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(value.longValue());
            return buffer.array();
        }
        if (data instanceof byte[]) {
            byte[] bytes = (byte[]) data;
            if (bytes.length == 1) {
                return bytes;
            }
            if (byteOrderOfBitType() == ByteOrder.BIG_ENDIAN) {
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
        if (data instanceof BitSet) {
            byte[] bytes = ((BitSet) data).toByteArray();
            return padLittleEndian(numBytes, bytes);
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    protected byte[] padLittleEndian(int numBytes, byte[] data) {
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
     * Determine whether the {@code byte[]} values for columns of type {@code BIT(n)} are {@link ByteOrder#BIG_ENDIAN big-endian}
     * or {@link ByteOrder#LITTLE_ENDIAN little-endian}. All values for {@code BIT(n)} columns are to be returned in
     * {@link ByteOrder#LITTLE_ENDIAN little-endian}.
     * <p>
     * By default, this method returns {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * @return little endian or big endian; never null
     */
    protected ByteOrder byteOrderOfBitType() {
        return ByteOrder.LITTLE_ENDIAN;
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
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            data = fieldDefn.schema().defaultValue();
        }
        if (data == null) {
            if (column.isOptional()) return null;
            return false;
        }
        if (data instanceof Boolean) return data;
        if (data instanceof Short) return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Integer) return ((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Long) return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Convert an unknown data value.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        if (column.isOptional() || fieldDefn.schema().isOptional()) {
            Class<?> dataClass = data.getClass();
            logger.warn("Unexpected value for JDBC type {} and column {}: class={}", column.jdbcType(), column,
                        dataClass.isArray() ? dataClass.getSimpleName() : dataClass.getName()); // don't include value in case its
                                                                                                // sensitive
            return null;
        }
        throw new IllegalArgumentException("Unexpected value for JDBC type " + column.jdbcType() + " and column " + column +
                ": class=" + data.getClass()); // don't include value in case its sensitive
    }
}

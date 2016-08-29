/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.data.Bits;
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

    private static final Short SHORT_TRUE = new Short((short) 1);
    private static final Short SHORT_FALSE = new Short((short) 0);
    private static final Integer INTEGER_TRUE = new Integer(1);
    private static final Integer INTEGER_FALSE = new Integer(0);
    private static final Long LONG_TRUE = new Long(1L);
    private static final Long LONG_FALSE = new Long(0L);
    private static final Float FLOAT_TRUE = new Float(1.0);
    private static final Float FLOAT_FALSE = new Float(0.0);
    private static final Double DOUBLE_TRUE = new Double(1.0d);
    private static final Double DOUBLE_FALSE = new Double(0.0d);

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final ZoneOffset defaultOffset;
    private final boolean adaptiveTimePrecision;

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones, and uses adapts time and timestamp values based upon the precision of the database
     * columns.
     */
    public JdbcValueConverters() {
        this(true, ZoneOffset.UTC);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values without timezone
     * information to values that require timezones. This default offset should not be needed when values are highly-correlated
     * with the expected SQL/JDBC types.
     * 
     * @param adaptiveTimePrecision {@code true} if the time, date, and timestamp values should be based upon the precision of the
     *            database columns using {@link io.debezium.time} semantic types, or {@code false} if they should be fixed to
     *            millisecond precision using Kafka Connect {@link org.apache.kafka.connect.data} logical types.
     * @param defaultOffset the zone offset that is to be used when converting non-timezone related values to values that do
     *            have timezones; may be null if UTC is to be used
     */
    public JdbcValueConverters(boolean adaptiveTimePrecision, ZoneOffset defaultOffset) {
        this.defaultOffset = defaultOffset != null ? defaultOffset : ZoneOffset.UTC;
        this.adaptiveTimePrecision = adaptiveTimePrecision;
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
                // values are fixed-precision decimal values with exact precision.
                // Use Kafka Connect's arbitrary precision decimal type and use the column's specified scale ...
                return Decimal.builder(column.scale());

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
            case Types.SQLXML:
                return SchemaBuilder.string();

            // Date and time values
            case Types.DATE:
                if (adaptiveTimePrecision) {
                return Date.builder();
                }
                return org.apache.kafka.connect.data.Date.builder();
            case Types.TIME:
                if (adaptiveTimePrecision) {
                    if (column.length() <= 3) return Time.builder();
                    if (column.length() <= 6) return MicroTime.builder();
                    return NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                if (adaptiveTimePrecision) {
                    if (column.length() <= 3 || !adaptiveTimePrecision) return Timestamp.builder();
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
                return (data) -> convertBit(column, fieldDefn, data);
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
                return (data) -> convertNumeric(column, fieldDefn, data);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data);

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
                if (adaptiveTimePrecision) {
                return (data) -> convertDateToEpochDays(column, fieldDefn, data);
                }
                return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data);
            case Types.TIME:
                if (adaptiveTimePrecision) {
                    if (column.length() <= 3) return (data) -> convertTimeToMillisPastMidnight(column, fieldDefn, data);
                    if (column.length() <= 6) return (data) -> convertTimeToMicrosPastMidnight(column, fieldDefn, data);
                    return (data) -> convertTimeToNanosPastMidnight(column, fieldDefn, data);
                }
                return (data) -> convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data);
            case Types.TIMESTAMP:
                if (adaptiveTimePrecision) {
                    if (column.length() <= 3) return (data) -> convertTimestampToEpochMillis(column, fieldDefn, data);
                    if (column.length() <= 6) return (data) -> convertTimestampToEpochMicros(column, fieldDefn, data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return ZonedTimestamp.toIsoString(data, defaultOffset);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeWithZone(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return ZonedTime.toIsoString(data, defaultOffset);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return Timestamp.toEpochMillis(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return MicroTimestamp.toEpochMicros(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return NanoTimestamp.toEpochNanos(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return new java.util.Date(Timestamp.toEpochMillis(data));
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeToMillisPastMidnight(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return Time.toMilliOfDay(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeToMicrosPastMidnight(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return MicroTime.toMicroOfDay(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeToNanosPastMidnight(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return NanoTime.toNanoOfDay(data);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertTimeToMillisPastMidnightAsDate(Column column, Field fieldDefn, Object data) {
        if ( data == null ) return null;
        try {
            return new java.util.Date(Time.toMilliOfDay(data));
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
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        try {
            return Date.toEpochDay(data);
        } catch (IllegalArgumentException e) {
            logger.warn("Unexpected JDBC DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                        fieldDefn.schema(), data.getClass(), data);
            return null;
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
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        try {
            int epochDay = Date.toEpochDay(data);
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
     * <p>
     * Per the JDBC specification, databases should return {@link java.sql.Date} instances that have no notion of time or
     * time zones. This method handles {@link java.sql.Date} objects plus any other standard date-related objects such as
     * {@link java.util.Date}, {@link java.time.LocalDate}, and {@link java.time.LocalDateTime}. If any of the types might
     * have time components, those time components are ignored.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBinary(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof char[]) {
            data = new String((char[]) data); // convert to string
        }
        if (data instanceof String) {
            // This was encoded as a hexadecimal string, but we receive it as a normal string ...
            data = ((String) data).getBytes();
        }
        if (data instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) data);
        }
        // An unexpected value
        return unexpectedBinary(data, fieldDefn);
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     * 
     * @param value the binary value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null
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
     * @return the converted value, or null if the conversion could not be made
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Short) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Short(value.shortValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? SHORT_TRUE : SHORT_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Integer) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Integer(value.intValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? INTEGER_TRUE : INTEGER_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Long) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Long(value.longValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? LONG_TRUE : LONG_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#FLOAT}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Double) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Double(value.doubleValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? DOUBLE_TRUE : DOUBLE_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#REAL}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Float) return data;
        if (data instanceof Number) {
            Number value = (Number) data;
            return new Float(value.floatValue());
        }
        if (data instanceof Boolean) {
            return ((Boolean) data).booleanValue() ? FLOAT_TRUE : FLOAT_FALSE;
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = new BigDecimal(((Boolean) data).booleanValue() ? 1 : 0);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        BigDecimal decimal = null;
        if (data instanceof BigDecimal)
            decimal = (BigDecimal) data;
        else if (data instanceof Boolean)
            decimal = new BigDecimal(((Boolean) data).booleanValue() ? 1 : 0);
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        return data == null ? null : data.toString();
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#ROWID}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertRowId(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
        if (data instanceof Boolean) return data;
        if (data instanceof Short) return ((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Integer) return ((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        if (data instanceof Long) return ((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        return handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#BOOLEAN}.
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        if (data == null) return null;
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
     * @return the converted value, or null if the conversion could not be made
     */
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        logger.warn("Unexpected value for JDBC type {} and column {}: class={}, value={}", column.jdbcType(), column,
                    data.getClass(), data);
        return null;
    }
}

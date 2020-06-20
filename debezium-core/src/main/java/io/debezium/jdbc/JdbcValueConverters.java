/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static io.debezium.jdbc.ConverterHelper.*;

import java.nio.ByteOrder;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.data.Bits;
import io.debezium.data.SpecialValueDecimal;
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
        PRECISE,
        DOUBLE,
        STRING
    }

    public enum BigIntUnsignedMode {
        PRECISE,
        LONG
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final ZoneOffset defaultOffset;

    /**
     * Fallback value for TIMESTAMP WITH TZ is epoch
     */
    protected final String fallbackTimestampWithTimeZone;

    /**
     * Fallback value for TIME WITH TZ is 00:00
     */
    protected final String fallbackTimeWithTimeZone;
    protected final boolean adaptiveTimePrecisionMode;
    protected final boolean adaptiveTimeMicrosecondsPrecisionMode;
    protected final DecimalMode decimalMode;
    protected final TemporalAdjuster adjuster;
    protected final BigIntUnsignedMode bigIntUnsignedMode;
    protected final BinaryHandlingMode binaryMode;

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones, and uses adapts time and timestamp values based upon the precision of the database
     * columns.
     */
    public JdbcValueConverters() {
        this(null, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, null);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values without timezone
     * information to values that require timezones. This default offset should not be needed when values are highly-correlated
     * with the expected SQL/JDBC types.
     *
     * @param decimalMode           how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *                              {@link DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param defaultOffset         the zone offset that is to be used when converting non-timezone related values to values that do
     *                              have timezones; may be null if UTC is to be used
     * @param adjuster              the optional component that adjusts the local date value before obtaining the epoch day; may be null if no
     *                              adjustment is necessary
     * @param bigIntUnsignedMode    how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *                              {@link BigIntUnsignedMode#PRECISE} is to be used
     * @param binaryMode            how binary columns should be represented
     */
    public JdbcValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                               TemporalAdjuster adjuster, BigIntUnsignedMode bigIntUnsignedMode, BinaryHandlingMode binaryMode) {
        this.defaultOffset = defaultOffset != null ? defaultOffset : ZoneOffset.UTC;
        this.adaptiveTimePrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE);
        this.adaptiveTimeMicrosecondsPrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        this.decimalMode = decimalMode != null ? decimalMode : DecimalMode.PRECISE;
        this.adjuster = adjuster;
        this.bigIntUnsignedMode = bigIntUnsignedMode != null ? bigIntUnsignedMode : BigIntUnsignedMode.PRECISE;
        this.binaryMode = binaryMode != null ? binaryMode : BinaryHandlingMode.BYTES;

        this.fallbackTimestampWithTimeZone = ZonedTimestamp.toIsoString(
                OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT, defaultOffset),
                defaultOffset,
                adjuster);
        this.fallbackTimeWithTimeZone = ZonedTime.toIsoString(
                OffsetTime.of(LocalTime.MIDNIGHT, defaultOffset),
                defaultOffset,
                adjuster);
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
                // Variable-length binary values ...
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return binaryMode.getSchema();

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
                return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().get());

            // Fixed-length string values
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
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
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return Time.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return MicroTime.builder();
                    }
                    return NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return Timestamp.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return MicroTimestamp.builder();
                    }
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
            case Types.DISTINCT:
            case Types.ARRAY:
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
                return convertBits(column, fieldDefn, byteOrderOfBitType());
            case Types.BOOLEAN:
                return (data) -> convertBoolean(column, fieldDefn, data);

            // Binary values ...
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return (data) -> convertBinary(column, fieldDefn, data, binaryMode);

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
                return (data) -> convertNumeric(column, fieldDefn, data, decimalMode);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data, decimalMode);

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
                    return (data) -> convertDateToEpochDays(column, fieldDefn, data, adjuster);
                }
                return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data, adjuster);
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return data -> convertTimeToMicrosPastMidnight(column, fieldDefn, data, supportsLargeTimeValues());
                }
                if (adaptiveTimePrecisionMode) {
                    if (column.length() <= 3) {
                        return data -> convertTimeToMillisPastMidnight(column, fieldDefn, data, supportsLargeTimeValues());
                    }
                    if (column.length() <= 6) {
                        return data -> convertTimeToMicrosPastMidnight(column, fieldDefn, data, supportsLargeTimeValues());
                    }
                    return data -> convertTimeToNanosPastMidnight(column, fieldDefn, data, supportsLargeTimeValues());
                }
                // "connect" mode
                else {
                    return data -> convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data, supportsLargeTimeValues());
                }
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return data -> convertTimestampToEpochMillis(column, fieldDefn, data, adjuster);
                    }
                    if (getTimePrecision(column) <= 6) {
                        return data -> convertTimestampToEpochMicros(column, fieldDefn, data, adjuster);
                    }
                    return (data) -> convertTimestampToEpochNanos(column, fieldDefn, data, adjuster);
                }
                return (data) -> convertTimestampToEpochMillisAsDate(column, fieldDefn, data, adjuster);
            case Types.TIME_WITH_TIMEZONE:
                return (data) -> convertTimeWithZone(column, fieldDefn, data, fallbackTimeWithTimeZone, defaultOffset, adjuster);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data, fallbackTimestampWithTimeZone, defaultOffset, adjuster);

            // Other types ...
            case Types.ROWID:
                return (data) -> convertRowId(column, fieldDefn, data);

            // Unhandled types
            case Types.DISTINCT:
            case Types.ARRAY:
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

    protected int getTimePrecision(Column column) {
        return column.length();
    }

    protected boolean supportsLargeTimeValues() {
        return adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode;
    }

}

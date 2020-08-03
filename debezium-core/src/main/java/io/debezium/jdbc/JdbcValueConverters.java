/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.nio.ByteOrder;
import java.sql.Types;
import java.time.*;
import java.time.temporal.TemporalAdjuster;
import java.util.function.Function;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
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

    final protected ValueConverterConfiguration configuration;

    protected static class ValueConverterConfiguration { // TODO DBZ-SOMETHING FIX! or work on making this an interface If necessary
        public final ZoneOffset defaultOffset;

        /**
         * Fallback value for TIMESTAMP WITH TZ is epoch
         */
        public final String fallbackTimestampWithTimeZone;

        /**
         * Fallback value for TIME WITH TZ is 00:00
         */
        public final String fallbackTimeWithTimeZone;
        public final boolean adaptiveTimePrecisionMode;
        public final boolean adaptiveTimeMicrosecondsPrecisionMode;
        public final DecimalMode decimalMode;
        public final TemporalAdjuster adjuster;
        public final BigIntUnsignedMode bigIntUnsignedMode;
        public final BinaryHandlingMode binaryMode;
        public final ByteOrder byteOrder;
        public final boolean supportsLargeTimeValues;

        public final Function<Column, Integer> timeprecision;

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
         * @param binaryMode how binary columns should be represented
         */
        public ValueConverterConfiguration(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                           TemporalAdjuster adjuster, BigIntUnsignedMode bigIntUnsignedMode, BinaryHandlingMode binaryMode, ByteOrder byteOrder,
                                           Function<Column, Integer> timeprecision) {
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
            this.byteOrder = byteOrder;
            this.supportsLargeTimeValues = adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode;
            this.timeprecision = timeprecision;
        }
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones, and uses adapts time and timestamp values based upon the precision of the database
     * columns.
     */
    public JdbcValueConverters() {
        this(null, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, null, ByteOrder.LITTLE_ENDIAN, Column::length);
    }

    public JdbcValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                               TemporalAdjuster adjuster, BigIntUnsignedMode bigIntUnsignedMode, BinaryHandlingMode binaryMode, ByteOrder byteOrder,
                               Function<Column, Integer> precision) {
        this.configuration = new ValueConverterConfiguration(decimalMode, temporalPrecisionMode, defaultOffset, adjuster, bigIntUnsignedMode, binaryMode, byteOrder,
                precision);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return ProxyConverter.fromJdbcType(column.jdbcType()).schemaBuilder.apply(column, this.configuration);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        return (data) -> ProxyConverter.fromJdbcType(column.jdbcType()).converter.apply(column, fieldDefn).apply(data, this.configuration);
    }
}

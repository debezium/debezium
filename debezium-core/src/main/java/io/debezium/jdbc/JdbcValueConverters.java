/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;


import java.nio.ByteOrder;
import java.sql.Types;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

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
        STRING;
    }

    public enum BigIntUnsignedMode {
        PRECISE,
        LONG;
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());



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
    public JdbcValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                               TemporalAdjuster adjuster, BigIntUnsignedMode bigIntUnsignedMode, BinaryHandlingMode binaryMode) {
        ConverterHelper.initiate(decimalMode, temporalPrecisionMode, defaultOffset, adjuster, bigIntUnsignedMode, binaryMode);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return ProxyConverter.fromJdbcType(column.jdbcType()).schemaBuilder.apply(column);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        return (data) -> ProxyConverter.fromJdbcType(column.jdbcType()).converter.convertType(column, fieldDefn, data);
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
    protected ByteOrder byteOrderOfBitType() { // should renove! unused
        ConverterHelper.byteOrderOfBitType = ByteOrder.LITTLE_ENDIAN;
        return ByteOrder.LITTLE_ENDIAN;
    }


    protected int getTimePrecision(Column column) {
        return column.length();
    }

}

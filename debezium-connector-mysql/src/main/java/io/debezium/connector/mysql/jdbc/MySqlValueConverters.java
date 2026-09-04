/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjuster;
import java.util.List;

import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;

import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.service.spi.ServiceRegistry;

/**
 * MySQL-specific customization of the conversions from JDBC values obtained from the MySQL binlog client library.
 * <p>
 * This class always uses UTC for the default time zone when converting values without timezone information to values that require
 * timezones. This is because MySQL {@code TIMESTAMP} values are always
 * <a href="https://dev.mysql.com/doc/refman/8.2/en/datetime.html">stored in UTC</a> (unlike {@code DATETIME} values) and
 * are replicated in this form. Meanwhile, the MySQL Binlog Client library will {@link AbstractRowsEventDataDeserializer
 * deserialize} these as {@link java.sql.Timestamp} values that have no timezone and, therefore, are presumed to be in UTC.
 * When the column is properly marked with a {@link Types#TIMESTAMP_WITH_TIMEZONE} type, the converters will need to convert
 * that {@link java.sql.Timestamp} value into an {@link OffsetDateTime} using the default time zone, which always is UTC.
 *
 * @author Randall Hauch
 */
@Immutable
public class MySqlValueConverters extends BinlogValueConverters {

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone information
     * to values that require timezones.
     * <p>
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode#PRECISE} is to be used
     * @param binaryMode how binary columns should be represented
     * @param adjuster a temporal adjuster to make a database specific time modification before conversion
     * @param eventConvertingFailureHandlingMode how handle when converting failure
     * @param serviceRegistry the service registry, should not be {@code null}
     */
    public MySqlValueConverters(DecimalMode decimalMode,
                                TemporalPrecisionMode temporalPrecisionMode,
                                BigIntUnsignedMode bigIntUnsignedMode,
                                BinaryHandlingMode binaryMode,
                                TemporalAdjuster adjuster,
                                EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode,
                                ServiceRegistry serviceRegistry) {
        super(decimalMode, temporalPrecisionMode, bigIntUnsignedMode, binaryMode, adjuster, eventConvertingFailureHandlingMode, serviceRegistry);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(Column column) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
    }
}

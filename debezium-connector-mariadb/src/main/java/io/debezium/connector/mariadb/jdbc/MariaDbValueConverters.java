/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import java.time.temporal.TemporalAdjuster;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;

/**
 * MariaDB specific converter handlers for JDBC values.<p></p>
 *
 * This class uses UTC for the default time zone when converting values without timezone details to values that
 * require timezones. This is because MariaDB {@code TIMESTAMP} values are always stored in UTC, unlike types
 * like {@code DATETIME}, and aare replicated as such. Meanwhile, the Binlog Client will deserialize these as
 * {@link java.sql.Timestamp} which have no timezone; therefore, are presumed to be UTC.<p></p>
 *
 * If a column is {@link java.sql.Types#TIMESTAMP_WITH_TIMEZONE}, the converters will need to convert the value
 * from a {@link java.sql.Timestamp} to an {@link java.time.OffsetDateTime} using the default time zone, which
 * is always UTC.
 *
 * @author Chris Cranford
 */
@Immutable
public class MariaDbValueConverters extends BinlogValueConverters {

    /**
     * Create a new instance of the value converters that always uses UTC for the default time zone when
     * converting values without timezone information to values that require timezones.
     *
     * @param decimalMode how {@code DECIMAL} and {@code NUMERIC} values are treated; can be null if {@link DecimalMode#PRECISE} is used
     * @param temporalPrecisionMode temporal precision mode
     * @param bigIntUnsignedMode how {@code BIGINT UNSIGNED} values are treated; may be null if {@link BigIntUnsignedMode#PRECISE} is used.
     * @param binaryHandlingMode how binary columns should be treated
     * @param adjuster a temporal adjuster to make a database specific time before conversion
     * @param eventConvertingFailureHandlingMode how to handle conversion failures
     * @param charsetRegistry the character set registry
     */
    public MariaDbValueConverters(DecimalMode decimalMode,
                                  TemporalPrecisionMode temporalPrecisionMode,
                                  BigIntUnsignedMode bigIntUnsignedMode,
                                  BinaryHandlingMode binaryHandlingMode,
                                  TemporalAdjuster adjuster,
                                  EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode,
                                  BinlogCharsetRegistry charsetRegistry) {
        super(decimalMode, temporalPrecisionMode, bigIntUnsignedMode, binaryHandlingMode, adjuster, eventConvertingFailureHandlingMode, charsetRegistry);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(Column column) {
        return MariaDbAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
    }
}

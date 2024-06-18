/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.time.temporal.TemporalAdjuster;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.BigIntUnsignedHandlingMode;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A helper factory for creating binlog value converter implementations for testing.
 *
 * @author Chris Cranford
 */
public interface BinlogValueConvertersFactory<T extends BinlogValueConverters> {
    /**
     * Creates an implementation of {@link BinlogValueConverters} based on a given configuration.
     *
     * @param configuration the connector configuraiton, should never be {@code null}.
     * @param temporalAdjuster the temporal adjuster, should never be {@code null}.
     *
     * @return the constructed value converter instance, never {@code null}
     */
    T create(Configuration configuration, TemporalAdjuster temporalAdjuster);

    /**
     * Creates an implementation of {@link BinlogValueConverters} for the given connector.
     *
     * @param decimalHandlingMode the decimal handling mode
     * @param temporalPrecisionMode the temporal precision mode
     * @param bigIntUnsignedHandlingMode the unsigned big integer handling mode
     * @param binaryHandlingMode the binary handling mode
     * @param temporalAdjuster the time adjuster implementation
     * @param eventConvertingFailureHandlingMode the event converting failure handling mode
     *
     * @return the constructed value converter instance, never {@code null}
     */
    default T create(RelationalDatabaseConnectorConfig.DecimalHandlingMode decimalHandlingMode,
                     TemporalPrecisionMode temporalPrecisionMode,
                     BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode,
                     BinaryHandlingMode binaryHandlingMode,
                     TemporalAdjuster temporalAdjuster,
                     EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        final Configuration configuration = Configuration.create()
                .with(BinlogConnectorConfig.DECIMAL_HANDLING_MODE, decimalHandlingMode)
                .with(BinlogConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .with(BinlogConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE, bigIntUnsignedHandlingMode)
                .with(BinlogConnectorConfig.BINARY_HANDLING_MODE, binaryHandlingMode)
                .with(BinlogConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, eventConvertingFailureHandlingMode)
                .build();

        return create(configuration, temporalAdjuster);
    }

    /**
     * Creates an implementation of {@link BinlogValueConverters} for the given connector.
     *
     * @param decimalHandlingMode the decimal handling mode
     * @param temporalPrecisionMode the temporal precision mode
     * @param bigIntUnsignedHandlingMode the unsigned big integer handling mode
     * @param binaryHandlingMode the binary handling mode
     * @param eventConvertingFailureHandlingMode the event converting failure handling mode
     *
     * @return the constructed value converter instance, never {@code null}
     */
    default T create(RelationalDatabaseConnectorConfig.DecimalHandlingMode decimalHandlingMode,
                     TemporalPrecisionMode temporalPrecisionMode,
                     BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode,
                     BinaryHandlingMode binaryHandlingMode,
                     EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        return create(decimalHandlingMode,
                temporalPrecisionMode,
                bigIntUnsignedHandlingMode,
                binaryHandlingMode,
                x -> x,
                eventConvertingFailureHandlingMode);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.temporal.TemporalAdjuster;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogValueConvertersTest;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Randall Hauch
 *
 */
public class MySqlValueConvertersTest extends BinlogValueConvertersTest<MySqlConnector> implements MySqlCommon {
    @Override
    protected BinlogValueConverters getValueConverters(JdbcValueConverters.DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode,
                                                       TemporalAdjuster temporalAdjuster,
                                                       EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        return new MySqlValueConverters(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryHandlingMode,
                temporalAdjuster,
                eventConvertingFailureHandlingMode);
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MySqlAntlrDdlParser();
    }
}

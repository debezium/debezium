/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * @author laomei
 */
public class MySqlDefaultValueTest extends BinlogDefaultValueTest<MySqlValueConverters, MySqlAntlrDdlParser> {
    @Override
    protected MySqlAntlrDdlParser getDdlParser(MySqlValueConverters valueConverter) {
        return new MySqlAntlrDdlParser();
    }

    @Override
    protected MySqlValueConverters getValueConverter(JdbcValueConverters.DecimalMode decimalMode,
                                                     TemporalPrecisionMode temporalPrecisionMode,
                                                     JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                     BinaryHandlingMode binaryHandlingMode) {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }
}
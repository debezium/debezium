/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.charset.MySqlCharsetRegistry;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * @author laomei
 */
public class MySqlDefaultValueTest extends BinlogDefaultValueTest<MySqlValueConverters, MySqlAntlrDdlParser> {
    @Override
    protected MySqlAntlrDdlParser getDdlParser(MySqlValueConverters valueConverter) {
        return new MySqlAntlrDdlParser(valueConverter);
    }

    @Override
    protected MySqlValueConverters getValueConverter(JdbcValueConverters.DecimalMode decimalMode,
                                                     TemporalPrecisionMode temporalPrecisionMode,
                                                     JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode,
                                                     BinaryHandlingMode binaryHandlingMode) {
        return new MySqlValueConverters(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryHandlingMode,
                x -> x,
                CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN,
                new MySqlCharsetRegistry());
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }
}
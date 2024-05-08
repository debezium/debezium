/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * @author Chris Cranford
 */
public class DefaultValueTest extends BinlogDefaultValueTest<MariaDbValueConverters, MariaDbAntlrDdlParser> {
    @Override
    protected MariaDbAntlrDdlParser getDdlParser(MariaDbValueConverters valueConverters) {
        return new MariaDbAntlrDdlParser(valueConverters);
    }

    @Override
    protected MariaDbValueConverters getValueConverter(DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode) {
        return new MariaDbValueConverters(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryHandlingMode,
                x -> x,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MariaDbValueConverters valueConverters) {
        return new MariaDbDefaultValueConverter(valueConverters);
    }
}

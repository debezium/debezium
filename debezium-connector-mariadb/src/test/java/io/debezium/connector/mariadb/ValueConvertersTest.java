/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.time.temporal.TemporalAdjuster;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogValueConvertersTest;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Chris Cranford
 */
public class ValueConvertersTest extends BinlogValueConvertersTest<MariaDbConnector> implements MariaDbCommon {
    @Override
    protected BinlogValueConverters getValueConverters(DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode,
                                                       TemporalAdjuster temporalAdjuster,
                                                       EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        return new MariaDbValueConverters(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryHandlingMode,
                temporalAdjuster,
                eventConvertingFailureHandlingMode,
                new MariaDbCharsetRegistry());
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import java.util.List;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.charset.MySqlCharsetRegistry;
import io.debezium.connector.mysql.jdbc.MySqlDefaultValueConverter;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParserTest
        extends BinlogAntlrDdlParserTest<MySqlValueConverters, MySqlDefaultValueConverter, MySqlAntlrDdlParser>
        implements MySqlCommon {
    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, MySqlValueConverters converters) {
        return new MySqlDdlParserWithSimpleTestListener(listener, converters);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, MySqlValueConverters converters, boolean includeViews) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews, converters);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, MySqlValueConverters converters, TableFilter tableFilter) {
        return new MySqlDdlParserWithSimpleTestListener(listener, tableFilter, converters);
    }

    @Override
    protected MySqlAntlrDdlParser getParser(SimpleDdlParserListener listener, MySqlValueConverters converters, boolean includeViews, boolean includeComments) {
        return new MySqlDdlParserWithSimpleTestListener(listener, includeViews, includeComments, converters);
    }

    @Override
    protected MySqlValueConverters getValueConverters() {
        return new MySqlValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(JdbcValueConverters.DecimalMode.DOUBLE.name()),
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(JdbcValueConverters.BigIntUnsignedMode.PRECISE.name()),
                BinaryHandlingMode.BYTES,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected MySqlDefaultValueConverter getDefaultValueConverters(MySqlValueConverters valueConverters) {
        return new MySqlDefaultValueConverter(valueConverters);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(List<String> enumValues) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(enumValues);
    }

    public static class MySqlDdlParserWithSimpleTestListener extends MySqlAntlrDdlParser {
        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, MySqlValueConverters converters) {
            this(changesListener, false, converters);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, TableFilter tableFilter, MySqlValueConverters converters) {
            this(changesListener, false, false, tableFilter, converters);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, MySqlValueConverters converters) {
            this(changesListener, includeViews, false, TableFilter.includeAll(), converters);
        }

        MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, MySqlValueConverters converters) {
            this(changesListener, includeViews, includeComments, TableFilter.includeAll(), converters);
        }

        private MySqlDdlParserWithSimpleTestListener(DdlChanges changesListener, boolean includeViews, boolean includeComments, TableFilter tableFilter,
                                                     MySqlValueConverters converters) {
            super(false, includeViews, includeComments, converters, tableFilter, new MySqlCharsetRegistry());
            this.ddlChanges = changesListener;
        }
    }
}

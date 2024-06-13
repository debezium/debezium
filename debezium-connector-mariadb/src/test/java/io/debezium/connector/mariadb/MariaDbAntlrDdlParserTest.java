/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author Chris Cranford
 */
public class MariaDbAntlrDdlParserTest extends BinlogAntlrDdlParserTest<MariaDbValueConverters, MariaDbDefaultValueConverter, MariaDbAntlrDdlParser> {
    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, MariaDbValueConverters converters) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, converters);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, MariaDbValueConverters converters, boolean includeViews) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, includeViews, converters);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, MariaDbValueConverters converters, Tables.TableFilter tableFilter) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, tableFilter, converters);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, MariaDbValueConverters converters, boolean includeViews, boolean includeComments) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, includeViews, includeComments, converters);
    }

    @Override
    protected MariaDbValueConverters getValueConverters() {
        return new MariaDbValueConverters(
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                x -> x,
                CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN,
                new MariaDbCharsetRegistry());
    }

    @Override
    protected MariaDbDefaultValueConverter getDefaultValueConverters(MariaDbValueConverters valueConverters) {
        return new MariaDbDefaultValueConverter(valueConverters);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(List<String> enumValues) {
        return MariaDbAntlrDdlParser.extractEnumAndSetOptions(enumValues);
    }

    public static class MariaDbDdlParserWithSimpleTestListener extends MariaDbAntlrDdlParser {
        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, MariaDbValueConverters converters) {
            this(listener, false, converters);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, Tables.TableFilter tableFilter, MariaDbValueConverters converters) {
            this(listener, false, false, tableFilter, converters);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, MariaDbValueConverters converters) {
            this(listener, includeViews, false, Tables.TableFilter.includeAll(), converters);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments, MariaDbValueConverters converters) {
            this(listener, includeViews, includeComments, Tables.TableFilter.includeAll(), converters);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments, Tables.TableFilter tableFilter,
                                                      MariaDbValueConverters converters) {
            super(false, includeViews, includeComments, converters, tableFilter, new MariaDbCharsetRegistry());
            this.ddlChanges = listener;
        }
    }
}

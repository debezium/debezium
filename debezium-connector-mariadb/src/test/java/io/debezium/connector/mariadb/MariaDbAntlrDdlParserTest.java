/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.BinlogAntlrDdlParserTest;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.charset.MariaDbCharsetRegistry;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author Chris Cranford
 */
public class MariaDbAntlrDdlParserTest extends BinlogAntlrDdlParserTest<MariaDbValueConverters, MariaDbDefaultValueConverter, MariaDbAntlrDdlParser> {
    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener) {
        return new MariaDbDdlParserWithSimpleTestListener(listener);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, includeViews);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, Tables.TableFilter tableFilter) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, tableFilter);
    }

    @Override
    protected MariaDbAntlrDdlParser getParser(SimpleDdlParserListener listener, boolean includeViews, boolean includeComments) {
        return new MariaDbDdlParserWithSimpleTestListener(listener, includeViews, includeComments);
    }

    @Override
    protected MariaDbValueConverters getValueConverters() {
        return new MariaDbValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.DOUBLE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.PRECISE,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN);
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
        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener) {
            this(listener, false);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, Tables.TableFilter tableFilter) {
            this(listener, false, false, tableFilter);
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews) {
            this(listener, includeViews, false, Tables.TableFilter.includeAll());
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments) {
            this(listener, includeViews, includeComments, Tables.TableFilter.includeAll());
        }

        public MariaDbDdlParserWithSimpleTestListener(DdlChanges listener, boolean includeViews, boolean includeComments, Tables.TableFilter tableFilter) {
            super(false, includeViews, includeComments, tableFilter, new MariaDbCharsetRegistry());
            this.ddlChanges = listener;
        }
    }
}

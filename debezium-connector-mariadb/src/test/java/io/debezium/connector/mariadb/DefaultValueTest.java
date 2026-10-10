/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDefaultValueTest;
import io.debezium.connector.binlog.jdbc.BinlogDefaultValueConverter;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.jdbc.MariaDbDefaultValueConverter;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * @author Chris Cranford
 */
public class DefaultValueTest extends BinlogDefaultValueTest<MariaDbValueConverters, MariaDbAntlrDdlParser> {
    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
            0      | 2000
            +0     | 2000
            -0     | 2000
            0000   | 2000
            0.0    | 2000
            0e0    | 2000
            '0'    | 2000
            '00'   | 2000
            18     | 2018
            69     | 2069
            70     | 1970
            99     | 1999
            """)
    @FixFor("debezium/dbz#2757")
    void shouldPreserveTwoDigitYearDefaults(String literal, Integer expected) {
        final var tableId = new TableId(null, null, "two_digit_year_defaults");
        final String definition = "y YEAR(2) DEFAULT " + literal;
        parser.parse("CREATE TABLE two_digit_year_defaults (" + definition + ")", tables);
        assertThat(tables.forTable(tableId).columnWithName("y").length()).isEqualTo(2);
        assertThat(getColumnSchema(tables.forTable(tableId), "y").defaultValue()).isEqualTo(expected);

        parser.parse("ALTER TABLE two_digit_year_defaults DROP COLUMN y", tables);
        parser.parse("ALTER TABLE two_digit_year_defaults ADD COLUMN " + definition, tables);
        assertThat(getColumnSchema(tables.forTable(tableId), "y").defaultValue()).isEqualTo(expected);

        for (String alteration : new String[]{ "MODIFY COLUMN " + definition,
                "CHANGE COLUMN y " + definition, "ALTER COLUMN y SET DEFAULT " + literal }) {
            parser.parse("ALTER TABLE two_digit_year_defaults MODIFY COLUMN y YEAR(2) DEFAULT 2018", tables);
            parser.parse("ALTER TABLE two_digit_year_defaults " + alteration, tables);
            assertThat(getColumnSchema(tables.forTable(tableId), "y").defaultValue()).as(alteration).isEqualTo(expected);
        }
    }

    @Override
    protected MariaDbAntlrDdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser();
    }

    @Override
    protected MariaDbValueConverters getValueConverter(DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode) {
        return new MariaDbValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                EventConvertingFailureHandlingMode.WARN);
    }

    @Override
    protected BinlogDefaultValueConverter getDefaultValueConverter(MariaDbValueConverters valueConverters) {
        return new MariaDbDefaultValueConverter(valueConverters);
    }
}

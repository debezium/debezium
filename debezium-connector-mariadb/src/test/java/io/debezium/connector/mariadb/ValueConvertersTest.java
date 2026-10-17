/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.Year;
import java.time.temporal.TemporalAdjuster;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogValueConvertersTest;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mariadb.antlr.MariaDbAntlrDdlParser;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.ddl.DdlParser;

/**
 * @author Chris Cranford
 */
public class ValueConvertersTest extends BinlogValueConvertersTest<MariaDbConnector> implements MariaDbCommon {
    @ParameterizedTest
    @MethodSource("twoDigitYearValues")
    @FixFor("debezium/dbz#2757")
    void shouldPreserveTwoDigitYearValues(Object value, Integer expected) {
        final var converters = getValueConverters(DecimalMode.PRECISE,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS, BigIntUnsignedMode.LONG,
                BinaryHandlingMode.BYTES, BinlogValueConverters::adjustTemporal, EventConvertingFailureHandlingMode.FAIL);
        final var column = Column.editor().name("y").type("YEAR").length(2).jdbcType(Types.INTEGER).optional(true).create();
        final var schema = converters.schemaBuilder(column).optional().build();
        final var converter = converters.converter(column, new Field("y", 0, schema));
        assertThat(converter.convert(value)).isEqualTo(expected);
    }

    static Stream<Arguments> twoDigitYearValues() {
        return Stream.of(Arguments.of(0, 2000), Arguments.of((short) 0, 2000),
                Arguments.of("0", 2000), Arguments.of("00", 2000),
                Arguments.of(18, 2018), Arguments.of(70, 1970), Arguments.of(Year.of(2000), 2000), Arguments.of(null, null));
    }

    @Override
    protected BinlogValueConverters getValueConverters(DecimalMode decimalMode,
                                                       TemporalPrecisionMode temporalPrecisionMode,
                                                       BigIntUnsignedMode bigIntUnsignedMode,
                                                       BinaryHandlingMode binaryHandlingMode,
                                                       TemporalAdjuster temporalAdjuster,
                                                       EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        return new MariaDbValueConvertersFactory().create(
                RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalMode.name()),
                temporalPrecisionMode,
                BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedMode.name()),
                binaryHandlingMode,
                temporalAdjuster,
                eventConvertingFailureHandlingMode);
    }

    @Override
    protected DdlParser getDdlParser() {
        return new MariaDbAntlrDdlParser();
    }
}

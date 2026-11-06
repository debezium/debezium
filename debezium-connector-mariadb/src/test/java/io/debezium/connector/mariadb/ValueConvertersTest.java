/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.temporal.TemporalAdjuster;

import org.apache.kafka.connect.data.Field;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.Configuration;
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
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
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

    @Test
    @FixFor("debezium/dbz#3006")
    void shouldConvertUnavailableValueToConfiguredPlaceholder() {
        final Configuration config = Configuration.create()
                .with(MariaDbConnectorConfig.TOPIC_PREFIX, "mariadb-server")
                .with(MariaDbConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER, "__debezium_unavailable_value")
                .build();
        final BinlogValueConverters converters = new MariaDbValueConvertersFactory().create(config, x -> x);

        final Tables tables = new Tables();
        getDdlParser().parse("CREATE TABLE NOBLOB_TABLE (ID INT PRIMARY KEY, C TEXT, B BLOB);", tables);
        final Table table = tables.forTable(new TableId(null, null, "NOBLOB_TABLE"));

        final Column textColumn = table.columnWithName("C");
        final Field textField = new Field(textColumn.name(), -1, converters.schemaBuilder(textColumn).optional().build());
        assertThat(converters.converter(textColumn, textField).convert(BinlogValueConverters.UNAVAILABLE_VALUE))
                .isEqualTo("__debezium_unavailable_value");

        final Column blobColumn = table.columnWithName("B");
        final Field blobField = new Field(blobColumn.name(), -1, converters.schemaBuilder(blobColumn).optional().build());
        assertThat(converters.converter(blobColumn, blobField).convert(BinlogValueConverters.UNAVAILABLE_VALUE))
                .isEqualTo(ByteBuffer.wrap("__debezium_unavailable_value".getBytes()));
    }
}

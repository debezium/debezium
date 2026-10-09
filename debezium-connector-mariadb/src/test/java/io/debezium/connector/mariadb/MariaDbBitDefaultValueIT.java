/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.binlog.BinlogBitDefaultValueIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BitDefaultValueTestCases;
import io.debezium.connector.binlog.BitDefaultValueTestCases.BitDefaultValueCase;
import io.debezium.connector.binlog.junit.BinlogDatabaseVersionResolver;
import io.debezium.data.Bits;
import io.debezium.doc.FixFor;

public class MariaDbBitDefaultValueIT extends BinlogBitDefaultValueIT<MariaDbConnector> implements MariaDbCommon {

    @ParameterizedTest
    @ValueSource(strings = { "1e30", "18446744073709551615e0" })
    @FixFor("debezium/dbz#2751")
    void shouldOmitUnknownBitDefaultAfterStreamingDdlAndRestart(String literal) throws Exception {
        final var database = database();
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, bits BIT(64) NOT NULL DEFAULT " + literal + ")",
                "INSERT INTO " + TABLE + " (id) VALUES (1)");
        final var config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();
        start(getConnectorClass(), config);

        // SHOW CREATE TABLE supplies the server's normalized binary default during the snapshot.
        assertOverflowDefaultAndValue(consumeRecord(1, "r"), 1, true);
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(), "ALTER TABLE " + TABLE + " ALTER COLUMN bits SET DEFAULT b'1'",
                "INSERT INTO " + TABLE + " (id) VALUES (2)");
        final var knownDefault = ((Struct) consumeRecord(2, "c").value()).getStruct("after");
        assertThat(asUnsignedDecimal(knownDefault.schema().field("bits").schema().defaultValue())).isEqualTo("1");

        executeStatements(database.getDatabaseName(), "ALTER TABLE " + TABLE + " ALTER COLUMN bits SET DEFAULT " + literal,
                "INSERT INTO " + TABLE + " (id) VALUES (3)");
        assertOverflowDefaultAndValue(consumeRecord(3, "c"), 3, false);

        stopConnector();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), database.getServerName());
        executeStatements(database.getDatabaseName(), "INSERT INTO " + TABLE + " (id) VALUES (4)");
        assertOverflowDefaultAndValue(consumeRecord(4, "c"), 4, false);
    }

    private void assertOverflowDefaultAndValue(SourceRecord record, int id, boolean snapshot) throws SQLException {
        final var after = ((Struct) record.value()).getStruct("after");
        final var schema = after.schema().field("bits").schema();
        try (var connection = getTestDatabaseConnection(database().getDatabaseName())) {
            connection.query("SELECT CAST(bits AS UNSIGNED) FROM " + TABLE + " WHERE id=" + id, result -> {
                assertThat(result.next()).isTrue();
                final var storedValue = result.getString(1);
                // Overflow results vary across MariaDB builds (MDEV-35715); use the actual stored value.
                // Only the streamed schema default is unknown; the captured row must remain unchanged.
                assertThat(asUnsignedDecimal(after.getWithoutDefault("bits"))).isEqualTo(storedValue);
                assertThat(asUnsignedDecimal(schema.defaultValue())).isEqualTo(snapshot ? storedValue : null);
                assertThat(result.next()).isFalse();
            });
        }
        assertThat(schema.isOptional()).isFalse();
        assertThat(schema.type()).isEqualTo(Schema.Type.BYTES);
        assertThat(schema.name()).isEqualTo(Bits.LOGICAL_NAME);
        assertThat(schema.parameters().get(Bits.LENGTH_FIELD)).isEqualTo("64");
    }

    @Override
    protected List<BitDefaultValueCase> bitDefaultCases() {
        final List<BitDefaultValueCase> cases = super.bitDefaultCases();
        if (new BinlogDatabaseVersionResolver().getVersion().isGreaterThanEqualTo(12, 3, -1)) {
            // MariaDB 12.3 rejects exact decimal defaults such as 0.5 and 1.5 with error 1067,
            // even with sql_mode=''. Exclude only these rejected literals from the valid-DDL tests.
            // Exponent-form 1.5e0 uses a different conversion and remains valid. Quoting 1.5 would
            // change BIT semantics to a byte string, so it cannot preserve the decimal rounding case.
            // Shared parser unit tests retain these literals for older DDL and schema history.
            return cases.stream().filter(testCase -> !isFractionalDecimal(testCase.literal())).toList();
        }
        return cases;
    }

    private static boolean isFractionalDecimal(String literal) {
        return literal.matches("[+-]?\\d+\\.\\d+") && new BigDecimal(literal).stripTrailingZeros().scale() > 0;
    }

    @Override
    protected List<BitDefaultValueCase> nonStrictBitDefaultCases() {
        return BitDefaultValueTestCases.mariaDbNonStrictCases().toList();
    }
}

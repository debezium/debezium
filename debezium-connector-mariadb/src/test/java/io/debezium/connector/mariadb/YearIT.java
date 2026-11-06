/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogYearIT;
import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
public class YearIT extends BinlogYearIT<MariaDbConnector> implements MariaDbCommon {

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2757")
    void shouldPreserveTwoDigitYearRows(boolean timeAdjusterEnabled) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE two_digit_year (id INT PRIMARY KEY, y YEAR(2))",
                "INSERT INTO two_digit_year VALUES (1, 2000), (2, 2018), (3, 1970), (4, 2069), (5, 1999), (6, NULL)");
        final Integer[] years = { 2000, 2018, 1970, 2069, 1999, null };
        try (var connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.query("SELECT YEAR(y) FROM two_digit_year ORDER BY id", result -> {
                for (Integer year : years) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getObject(1) == null ? null : result.getInt(1)).isEqualTo(year);
                }
            });
        }
        start(getConnectorClass(), twoDigitYearConfig(timeAdjusterEnabled));
        for (int i = 0; i < years.length; i++) {
            assertThat(consumeTwoDigitYear(i + 1, "r").getStruct("after").getWithoutDefault("y")).isEqualTo(years[i]);
        }
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO two_digit_year SELECT id + " + years.length + ", y FROM two_digit_year");
        for (int i = 0; i < years.length; i++) {
            assertThat(consumeTwoDigitYear(i + years.length + 1, "c").getStruct("after").getWithoutDefault("y")).isEqualTo(years[i]);
        }

        final int updatedId = years.length + 2;
        executeStatements(DATABASE.getDatabaseName(), "UPDATE two_digit_year SET y = 0 WHERE id = " + updatedId);
        final var update = consumeTwoDigitYear(updatedId, "u");
        assertThat(update.getStruct("before").getWithoutDefault("y")).isEqualTo(2018);
        assertThat(update.getStruct("after").getWithoutDefault("y")).isEqualTo(2000);
        executeStatements(DATABASE.getDatabaseName(), "DELETE FROM two_digit_year WHERE id = " + updatedId);
        assertThat(consumeTwoDigitYear(updatedId, "d").getStruct("before").getWithoutDefault("y")).isEqualTo(2000);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2757")
    void shouldPreserveTwoDigitYearDefaults(boolean timeAdjusterEnabled) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE two_digit_year (id INT PRIMARY KEY, y YEAR(2) DEFAULT 0)",
                "INSERT INTO two_digit_year (id) VALUES (1)");
        final var config = twoDigitYearConfig(timeAdjusterEnabled);
        start(getConnectorClass(), config);
        assertTwoDigitYearDefault(1, "r", 2000);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        final String[] literals = { "0", "'0'", "'00'", "69", "70" };
        final Integer[] years = { 2000, 2000, 2000, 2069, 1970 };
        int id = 2;
        for (String alteration : new String[]{ "MODIFY COLUMN y YEAR(2) DEFAULT ", "ALTER COLUMN y SET DEFAULT " }) {
            for (int i = 0; i < literals.length; i++) {
                executeStatements(DATABASE.getDatabaseName(),
                        "ALTER TABLE two_digit_year " + alteration + literals[i],
                        "INSERT INTO two_digit_year (id) VALUES (" + id + ")");
                assertTwoDigitYearDefault(id++, "c", years[i]);
            }
        }

        executeStatements(DATABASE.getDatabaseName(),
                "ALTER TABLE two_digit_year ALTER COLUMN y SET DEFAULT 0",
                "INSERT INTO two_digit_year (id) VALUES (" + id + ")");
        assertTwoDigitYearDefault(id++, "c", 2000);
        stopConnector();
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO two_digit_year (id) VALUES (" + id + ")");
        start(getConnectorClass(), config);
        assertTwoDigitYearDefault(id, "c", 2000);
    }

    private Configuration twoDigitYearConfig(boolean timeAdjusterEnabled) {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.ENABLE_TIME_ADJUSTER, timeAdjusterEnabled)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("two_digit_year"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();
    }

    private Struct consumeTwoDigitYear(int id, String operation) throws InterruptedException {
        final var records = consumeRecordsByTopic(1).recordsForTopic(DATABASE.topicForTable("two_digit_year"));
        assertThat(records).hasSize(1);
        final var envelope = (Struct) records.get(0).value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        final var row = envelope.getStruct("d".equals(operation) ? "before" : "after");
        assertThat(row.getInt32("id")).isEqualTo(id);
        assertThat(row.schema().field("y").schema().name()).isEqualTo("io.debezium.time.Year");
        return envelope;
    }

    private void assertTwoDigitYearDefault(int id, String operation, Integer expected) throws Exception {
        final var row = consumeTwoDigitYear(id, operation).getStruct("after");
        assertThat(row.schema().field("y").schema().defaultValue()).isEqualTo(expected);
        assertThat(row.getWithoutDefault("y")).isEqualTo(expected);
        try (var connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.query("SELECT YEAR(y) FROM two_digit_year WHERE id = " + id, result -> {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isEqualTo(expected);
            });
        }
    }
}

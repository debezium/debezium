/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * Verify conversions around 2 and 4 digit year values.
 *
 * @author Jiri Pechanec
 */
public abstract class BinlogYearIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-year.txt")
            .toAbsolutePath();
    protected final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("yearit", "year_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2757")
    void shouldPreserveZeroYearsAndDefaults(boolean timeAdjusterEnabled) throws Exception {
        executeStatements(DATABASE.getDatabaseName(),
                "CREATE TABLE zero_year (id INT PRIMARY KEY, y YEAR NULL DEFAULT 0, quoted_year YEAR DEFAULT '0')",
                "INSERT INTO zero_year (id, y) VALUES (1, 0), (2, '0'), (3, 1901), (4, 2155), (5, NULL)");
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.ENABLE_TIME_ADJUSTER, timeAdjusterEnabled)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("zero_year"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();
        start(getConnectorClass(), config);

        final Integer[] expected = { 0, 2000, 1901, 2155, null };
        for (int i = 0; i < expected.length; i++) {
            assertYearInsert(i + 1, "r", expected[i], 0);
        }
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO zero_year (id) VALUES (6)");
        assertYearInsert(6, "c", 0, 0);

        executeStatements(DATABASE.getDatabaseName(), "UPDATE zero_year SET y = 2026 WHERE id = 6");
        final var update = consumeYearEnvelope(6, "u");
        assertThat(update.getStruct("before").getWithoutDefault("y")).isEqualTo(0);
        assertThat(update.getStruct("after").getWithoutDefault("y")).isEqualTo(2026);
        executeStatements(DATABASE.getDatabaseName(), "UPDATE zero_year SET y = 0 WHERE id = 6");
        final var reset = consumeYearEnvelope(6, "u");
        assertThat(reset.getStruct("before").getWithoutDefault("y")).isEqualTo(2026);
        assertThat(reset.getStruct("after").getWithoutDefault("y")).isEqualTo(0);
        executeStatements(DATABASE.getDatabaseName(), "DELETE FROM zero_year WHERE id = 6");
        assertThat(consumeYearEnvelope(6, "d").getStruct("before").getWithoutDefault("y")).isEqualTo(0);

        final String[] literals = { "0", "'0'", "'00'", "'0000'", "69", "70" };
        final int[] years = { 0, 2000, 2000, 0, 2069, 1970 };
        int id = 7;
        for (String alteration : new String[]{ "MODIFY COLUMN y YEAR NULL DEFAULT ", "ALTER COLUMN y SET DEFAULT " }) {
            for (int i = 0; i < literals.length; i++) {
                executeStatements(DATABASE.getDatabaseName(),
                        "ALTER TABLE zero_year " + alteration + literals[i],
                        "INSERT INTO zero_year (id) VALUES (" + id + ")");
                assertYearInsert(id++, "c", years[i], years[i]);
            }
        }
        executeStatements(DATABASE.getDatabaseName(),
                "ALTER TABLE zero_year MODIFY COLUMN y YEAR NULL DEFAULT NULL",
                "INSERT INTO zero_year (id) VALUES (" + id + ")");
        assertYearInsert(id++, "c", null, null);

        executeStatements(DATABASE.getDatabaseName(),
                "ALTER TABLE zero_year ALTER COLUMN y SET DEFAULT 0",
                "INSERT INTO zero_year (id) VALUES (" + id + ")");
        assertYearInsert(id++, "c", 0, 0);
        stopConnector();
        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO zero_year (id) VALUES (" + id + ")");
        start(getConnectorClass(), config);
        assertYearInsert(id, "c", 0, 0);
    }

    private Struct consumeYearEnvelope(int id, String operation) throws InterruptedException {
        final var records = consumeRecordsByTopic(1).recordsForTopic(DATABASE.topicForTable("zero_year"));
        assertThat(records).hasSize(1);
        final var envelope = (Struct) records.get(0).value();
        assertThat(envelope.getString("op")).isEqualTo(operation);
        final var row = envelope.getStruct("d".equals(operation) ? "before" : "after");
        assertThat(row.getInt32("id")).isEqualTo(id);
        return envelope;
    }

    private void assertYearInsert(int id, String operation, Integer expected, Integer defaultValue) throws Exception {
        final var row = consumeYearEnvelope(id, operation).getStruct("after");
        assertThat(row.schema().field("y").schema().name()).isEqualTo("io.debezium.time.Year");
        assertThat(row.schema().field("y").schema().defaultValue()).isEqualTo(defaultValue);
        assertThat(row.getWithoutDefault("y")).isEqualTo(expected);
        assertThat(row.get("y")).isEqualTo(expected == null ? defaultValue : expected);
        assertThat(row.schema().field("quoted_year").schema().defaultValue()).isEqualTo(2000);
        assertThat(row.getWithoutDefault("quoted_year")).isEqualTo(2000);

        try (var connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.query("SELECT CAST(y AS SIGNED) FROM zero_year WHERE id = " + id, result -> {
                assertThat(result.next()).isTrue();
                final Integer year = result.getObject(1) == null ? null : result.getInt(1);
                assertThat(year).isEqualTo(expected);
            });
        }
    }

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1143")
    public void shouldProcessTwoAndForDigitYearsInDatabase() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.ENABLE_TIME_ADJUSTER, false)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        final int numDatabase = 2;
        final int numTables = 2;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);

        assertChangeRecordByDatabase();

        try (Connection conn = getTestDatabaseConnection(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO dbz_1143_year_test VALUES (\n" +
                    "    default,\n" +
                    "    '18',\n" +
                    "    '0018',\n" +
                    "    '2018',\n" +
                    "    '18-04-01',\n" +
                    "    '0018-04-01',\n" +
                    "    '2018-04-01',\n" +
                    "    '18-04-01 12:34:56',\n" +
                    "    '0018-04-01 12:34:56',\n" +
                    "    '2018-04-01 12:34:56',\n" +
                    "    '78',\n" +
                    "    '0078',\n" +
                    "    '1978',\n" +
                    "    '78-04-01',\n" +
                    "    '0078-04-01',\n" +
                    "    '1978-04-01',\n" +
                    "    '78-04-01 12:34:56',\n" +
                    "    '0078-04-01 12:34:56',\n" +
                    "    '1978-04-01 12:34:56'" +
                    ");");
        }

        assertChangeRecordByDatabase();
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1143")
    public void shouldProcessTwoAndForDigitYearsInConnector() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        final int numDatabase = 2;
        final int numTables = 2;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);

        assertChangeRecordByConnector();

        try (Connection conn = getTestDatabaseConnection(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO dbz_1143_year_test VALUES (\n" +
                    "    default,\n" +
                    "    '18',\n" +
                    "    '0018',\n" +
                    "    '2018',\n" +
                    "    '18-04-01',\n" +
                    "    '0018-04-01',\n" +
                    "    '2018-04-01',\n" +
                    "    '18-04-01 12:34:56',\n" +
                    "    '0018-04-01 12:34:56',\n" +
                    "    '2018-04-01 12:34:56',\n" +
                    "    '78',\n" +
                    "    '0078',\n" +
                    "    '1978',\n" +
                    "    '78-04-01',\n" +
                    "    '0078-04-01',\n" +
                    "    '1978-04-01',\n" +
                    "    '78-04-01 12:34:56',\n" +
                    "    '0078-04-01 12:34:56',\n" +
                    "    '1978-04-01 12:34:56'" +
                    ");");
        }

        assertChangeRecordByConnector();
        stopConnector();
    }

    private void assertChangeRecordByDatabase() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        // YEAR does not differentiate between 0018 and 18
        assertThat(change.getInt32("y18")).isEqualTo(2018);
        assertThat(change.getInt32("y0018")).isEqualTo(2018);
        assertThat(change.getInt32("y2018")).isEqualTo(2018);

        // days elapsed since epoch till 2018-04-01
        assertThat(change.getInt32("d18")).isEqualTo(17622);
        // days counted backward from epoch to 0018-04-01
        assertThat(change.getInt32("d0018")).isEqualTo(-712863);
        // days elapsed since epoch till 2018-04-01
        assertThat(change.getInt32("d2018")).isEqualTo(17622);

        // nanos elapsed since epoch till 2018-04-01
        assertThat(change.getInt64("dt18")).isEqualTo(1_522_586_096_000L);
        // Assert for 0018 will not work as long is able to handle only 292 years of nanos so we are underflowing
        // nanos elapsed since epoch till 2018-04-01
        assertThat(change.getInt64("dt2018")).isEqualTo(1_522_586_096_000L);

        // YEAR does not differentiate between 0078 and 78
        assertThat(change.getInt32("y78")).isEqualTo(1978);
        assertThat(change.getInt32("y0078")).isEqualTo(1978);
        assertThat(change.getInt32("y1978")).isEqualTo(1978);

        // days elapsed since epoch till 1978-04-01
        assertThat(change.getInt32("d78")).isEqualTo(3012);
        // days counted backward from epoch to 0078-04-01
        assertThat(change.getInt32("d0078")).isEqualTo(-690948);
        // days elapsed since epoch till 1978-04-01
        assertThat(change.getInt32("d1978")).isEqualTo(3012);

        // nanos elapsed since epoch till 1978-04-01
        assertThat(change.getInt64("dt78")).isEqualTo(260_282_096_000L);
        // Assert for 0018 will not work as long is able to handle only 292 years of nanos so we are underflowing
        // nanos elapsed since epoch till 1978-04-01
        assertThat(change.getInt64("dt1978")).isEqualTo(260_282_096_000L);
    }

    private void assertChangeRecordByConnector() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        // YEAR does not differentiate between 0018 and 18
        assertThat(change.getInt32("y18")).isEqualTo(2018);
        assertThat(change.getInt32("y0018")).isEqualTo(2018);
        assertThat(change.getInt32("y2018")).isEqualTo(2018);

        // days elapsed since epoch till 2018-04-01
        assertThat(change.getInt32("d18")).isEqualTo(17622);
        assertThat(change.getInt32("d0018")).isEqualTo(17622);
        assertThat(change.getInt32("d2018")).isEqualTo(17622);

        // nanos elapsed since epoch till 2018-04-01
        assertThat(change.getInt64("dt18")).isEqualTo(1_522_586_096_000L);
        assertThat(change.getInt64("dt0018")).isEqualTo(1_522_586_096_000L);
        assertThat(change.getInt64("dt2018")).isEqualTo(1_522_586_096_000L);

        // YEAR does not differentiate between 0078 and 78
        assertThat(change.getInt32("y78")).isEqualTo(1978);
        assertThat(change.getInt32("y0078")).isEqualTo(1978);
        assertThat(change.getInt32("y1978")).isEqualTo(1978);

        // days elapsed since epoch till 1978-04-01
        assertThat(change.getInt32("d78")).isEqualTo(3012);
        assertThat(change.getInt32("d0078")).isEqualTo(3012);
        assertThat(change.getInt32("d1978")).isEqualTo(3012);

        // nanos elapsed since epoch till 1978-04-01
        assertThat(change.getInt64("dt78")).isEqualTo(260_282_096_000L);
        assertThat(change.getInt64("dt0078")).isEqualTo(260_282_096_000L);
        assertThat(change.getInt64("dt1978")).isEqualTo(260_282_096_000L);
    }
}

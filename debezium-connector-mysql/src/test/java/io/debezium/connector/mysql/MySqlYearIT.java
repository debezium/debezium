/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Verify conversions around 2 and 4 digit year values.
 *
 * @author Jiri Pechanec
 */
public class MySqlYearIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-year.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("yearit", "year_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1143")
    public void shouldProcessTwoAndForDigitYearsInDatabase() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER, false)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        Testing.Debug.enable();
        final int numDatabase = 2;
        final int numTables = 2;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);

        assertChangeRecordByDatabase();

        try (final Connection conn = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
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
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        Testing.Debug.enable();
        final int numDatabase = 2;
        final int numTables = 2;
        final int numOthers = 2;
        consumeRecords(numDatabase + numTables + numOthers);

        assertChangeRecordByConnector();

        try (final Connection conn = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
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
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        // YEAR does not differentiate between 0018 and 18
        Assertions.assertThat(change.getInt32("y18")).isEqualTo(2018);
        Assertions.assertThat(change.getInt32("y0018")).isEqualTo(2018);
        Assertions.assertThat(change.getInt32("y2018")).isEqualTo(2018);

        // days elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt32("d18")).isEqualTo(17622);
        // days counted backward from epoch to 0018-04-01
        Assertions.assertThat(change.getInt32("d0018")).isEqualTo(-712863);
        // days elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt32("d2018")).isEqualTo(17622);

        // nanos elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt64("dt18")).isEqualTo(1_522_586_096_000L);
        // Assert for 0018 will not work as long is able to handle only 292 years of nanos so we are underflowing
        // nanos elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt64("dt2018")).isEqualTo(1_522_586_096_000L);

        // YEAR does not differentiate between 0078 and 78
        Assertions.assertThat(change.getInt32("y78")).isEqualTo(1978);
        Assertions.assertThat(change.getInt32("y0078")).isEqualTo(1978);
        Assertions.assertThat(change.getInt32("y1978")).isEqualTo(1978);

        // days elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt32("d78")).isEqualTo(3012);
        // days counted backward from epoch to 0078-04-01
        Assertions.assertThat(change.getInt32("d0078")).isEqualTo(-690948);
        // days elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt32("d1978")).isEqualTo(3012);

        // nanos elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt64("dt78")).isEqualTo(260_282_096_000L);
        // Assert for 0018 will not work as long is able to handle only 292 years of nanos so we are underflowing
        // nanos elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt64("dt1978")).isEqualTo(260_282_096_000L);
    }

    private void assertChangeRecordByConnector() throws InterruptedException {
        final SourceRecord record = consumeRecord();
        Assertions.assertThat(record).isNotNull();
        final Struct change = ((Struct) record.value()).getStruct("after");

        // YEAR does not differentiate between 0018 and 18
        Assertions.assertThat(change.getInt32("y18")).isEqualTo(2018);
        Assertions.assertThat(change.getInt32("y0018")).isEqualTo(2018);
        Assertions.assertThat(change.getInt32("y2018")).isEqualTo(2018);

        // days elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt32("d18")).isEqualTo(17622);
        Assertions.assertThat(change.getInt32("d0018")).isEqualTo(17622);
        Assertions.assertThat(change.getInt32("d2018")).isEqualTo(17622);

        // nanos elapsed since epoch till 2018-04-01
        Assertions.assertThat(change.getInt64("dt18")).isEqualTo(1_522_586_096_000L);
        Assertions.assertThat(change.getInt64("dt0018")).isEqualTo(1_522_586_096_000L);
        Assertions.assertThat(change.getInt64("dt2018")).isEqualTo(1_522_586_096_000L);

        // YEAR does not differentiate between 0078 and 78
        Assertions.assertThat(change.getInt32("y78")).isEqualTo(1978);
        Assertions.assertThat(change.getInt32("y0078")).isEqualTo(1978);
        Assertions.assertThat(change.getInt32("y1978")).isEqualTo(1978);

        // days elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt32("d78")).isEqualTo(3012);
        Assertions.assertThat(change.getInt32("d0078")).isEqualTo(3012);
        Assertions.assertThat(change.getInt32("d1978")).isEqualTo(3012);

        // nanos elapsed since epoch till 1978-04-01
        Assertions.assertThat(change.getInt64("dt78")).isEqualTo(260_282_096_000L);
        Assertions.assertThat(change.getInt64("dt0078")).isEqualTo(260_282_096_000L);
        Assertions.assertThat(change.getInt64("dt1978")).isEqualTo(260_282_096_000L);
    }
}

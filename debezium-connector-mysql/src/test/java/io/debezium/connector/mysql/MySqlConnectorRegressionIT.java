/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorRegressionIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-regression.txt").toAbsolutePath();

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "regression")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "regression_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .with("database.useSSL", false) // eliminates MySQL driver warning about SSL connections
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        SourceRecords records = consumeRecordsByTopic(4 + 3); // 4 schema change record, 3 inserts
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(4);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(4);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase("regression_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record->{
            Struct value = (Struct)record.value();
            if ( record.topic().endsWith("dbz_85_fractest")) {
                // The microseconds of all three should be exactly 780
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                java.util.Date c1 = (java.util.Date)after.get("c1");
                java.util.Date c2 = (java.util.Date)after.get("c2");
                java.util.Date c3 = (java.util.Date)after.get("c3");
                java.util.Date c4 = (java.util.Date)after.get("c4");
                Testing.debug("c1 = " + c1.getTime());
                Testing.debug("c2 = " + c2.getTime());
                Testing.debug("c3 = " + c3.getTime());
                Testing.debug("c4 = " + c4.getTime());
                assertThat(c1.getTime() % 1000).isEqualTo(0);   // date only, no time
                assertThat(c2.getTime() % 1000).isEqualTo(780);
                assertThat(c3.getTime() % 1000).isEqualTo(780);
                assertThat(c4.getTime() % 1000).isEqualTo(780);
                ZoneId utc = ZoneId.of("UTC");
                ZoneId defaultTZ = ZoneId.systemDefault();
                LocalDate expectedDate = LocalDate.of(2014, 9, 8);
                // the time is stored as 17:51:04.777 but rounded up to 780 due to the column configs
                LocalTime expectedTime = LocalTime.of(17, 51, 4).plus(780, ChronoUnit.MILLIS);
                // c1 '2014-09-08' is stored as a MySQL DATE (without any time) in the local TZ and then converted to 
                // a truncated UTC by the connector, so we must assert against the same thing....
                ZonedDateTime expectedC1UTC = ZonedDateTime.of(expectedDate, LocalTime.of(0, 0), defaultTZ)
                                                           .withZoneSameInstant(utc)
                                                           .truncatedTo(ChronoUnit.DAYS);
                assertThat(c1.getTime()).isEqualTo(expectedC1UTC.toInstant().toEpochMilli());
                ZonedDateTime expectedC2UTC = ZonedDateTime.of(LocalDate.ofEpochDay(0), expectedTime, utc);                         
                assertThat(c2.getTime()).isEqualTo(expectedC2UTC.toInstant().toEpochMilli());
                ZonedDateTime expectedC3UTC = ZonedDateTime.of(expectedDate, expectedTime, utc);
                assertThat(c3.getTime()).isEqualTo(expectedC3UTC.toInstant().toEpochMilli());
                assertThat(c4.getTime()).isEqualTo(expectedC3UTC.toInstant().toEpochMilli());
                // None of these Dates have timezone information, so to convert to locals we have to use our local timezone ...
                LocalDate localC1 = c1.toInstant().atZone(utc).toLocalDate();
                LocalTime localC2 = c2.toInstant().atZone(utc).toLocalTime();
                LocalDateTime localC3 = c3.toInstant().atZone(utc).toLocalDateTime();
                LocalDateTime localC4 = c4.toInstant().atZone(utc).toLocalDateTime();
                // row is ('2014-09-08', '17:51:04.78', '2014-09-08 17:51:04.78', '2014-09-08 17:51:04.78')
                final int expectedNanos = 780 * 1000 * 1000;
                assertThat(localC1.getYear()).isEqualTo(2014);
                assertThat(localC1.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(localC1.getDayOfMonth()).isEqualTo(expectedC1UTC.get(ChronoField.DAY_OF_MONTH));
                assertThat(localC2.getHour()).isEqualTo(17);
                assertThat(localC2.getMinute()).isEqualTo(51);
                assertThat(localC2.getSecond()).isEqualTo(4);
                assertThat(localC2.getNano()).isEqualTo(expectedNanos);
                assertThat(localC3.getYear()).isEqualTo(2014);
                assertThat(localC3.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(localC3.getDayOfMonth()).isEqualTo(8);
                assertThat(localC3.getHour()).isEqualTo(17);
                assertThat(localC3.getMinute()).isEqualTo(51);
                assertThat(localC3.getSecond()).isEqualTo(4);
                assertThat(localC3.getNano()).isEqualTo(expectedNanos);
                assertThat(localC4.getYear()).isEqualTo(2014);
                assertThat(localC4.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(localC4.getDayOfMonth()).isEqualTo(8);
                assertThat(localC4.getHour()).isEqualTo(17);
                assertThat(localC4.getMinute()).isEqualTo(51);
                assertThat(localC4.getSecond()).isEqualTo(4);
                assertThat(localC4.getNano()).isEqualTo(expectedNanos);
            }
        });
    }

}

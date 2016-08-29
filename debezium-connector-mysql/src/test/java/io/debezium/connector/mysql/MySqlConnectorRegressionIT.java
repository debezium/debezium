/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.TemporalPrecisionMode;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.time.ZonedTimestamp;
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
    @FixFor("DBZ-61")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "regression")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "regression_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        SourceRecords records = consumeRecordsByTopic(6 + 7); // 5 schema change record, 7 inserts
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(6);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(6);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase("regression_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_100_enumsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String c1 = after.getString("c1");
                String c2 = after.getString("c2");
                if (c1.equals("a")) {
                    assertThat(c2).isEqualTo("a,b,c");
                } else if (c1.equals("b")) {
                    assertThat(c2).isEqualTo("a,b");
                } else if (c1.equals("c")) {
                    assertThat(c2).isEqualTo("a");
                } else {
                    fail("c1 didn't match expected value");
                }
            } else if (record.topic().endsWith("dbz_102_charsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String text = after.getString("text");
                assertThat(text).isEqualTo("产品");
            } else if (record.topic().endsWith("dbz_85_fractest")) {
                // The microseconds of all three should be exactly 780
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 DATE,
                // c2 TIME(2),
                // c3 DATETIME(2),
                // c4 TIMESTAMP(2)
                //
                // {"c1" : "16321", "c2" : "64264780", "c3" : "1410198664780", "c4" : "2014-09-08T17:51:04.78-05:00"}

                // '2014-09-08'
                Integer c1 = after.getInt32("c1"); // epoch days
                LocalDate c1Date = LocalDate.ofEpochDay(c1);
                assertThat(c1Date.getYear()).isEqualTo(2014);
                assertThat(c1Date.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c1Date.getDayOfMonth()).isEqualTo(8);
                assertThat(io.debezium.time.Date.toEpochDay(c1Date)).isEqualTo(c1);

                // '17:51:04.777'
                Integer c2 = after.getInt32("c2"); // milliseconds past midnight
                LocalTime c2Time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(c2));
                assertThat(c2Time.getHour()).isEqualTo(17);
                assertThat(c2Time.getMinute()).isEqualTo(51);
                assertThat(c2Time.getSecond()).isEqualTo(4);
                assertThat(c2Time.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time)).isEqualTo(c2);

                // '2014-09-08 17:51:04.777'
                Long c3 = after.getInt64("c3"); // epoch millis
                long c3Seconds = c3 / 1000;
                long c3Millis = c3 % 1000;
                LocalDateTime c3DateTime = LocalDateTime.ofEpochSecond(c3Seconds,
                                                                       (int) TimeUnit.MILLISECONDS.toNanos(c3Millis),
                                                                       ZoneOffset.UTC);
                assertThat(c3DateTime.getYear()).isEqualTo(2014);
                assertThat(c3DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c3DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c3DateTime.getHour()).isEqualTo(17);
                assertThat(c3DateTime.getMinute()).isEqualTo(51);
                assertThat(c3DateTime.getSecond()).isEqualTo(4);
                assertThat(c3DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime)).isEqualTo(c3);

                // '2014-09-08 17:51:04.777'
                String c4 = after.getString("c4"); // timestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(OffsetDateTime.now().getOffset());
                assertThat(c4DateTime.getYear()).isEqualTo(2014);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c4DateTime.getHour()).isEqualTo(17);
                assertThat(c4DateTime.getMinute()).isEqualTo(51);
                assertThat(c4DateTime.getSecond()).isEqualTo(4);
                assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            }
        });
    }

    @Test
    @FixFor("DBZ-61")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshotAndConnectTimesTypes() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "regression")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "regression_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(MySqlConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT.toString())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        SourceRecords records = consumeRecordsByTopic(6 + 7); // 6 schema change record, 7 inserts
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(6);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(6);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(6);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase("regression_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_100_enumsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String c1 = after.getString("c1");
                String c2 = after.getString("c2");
                if (c1.equals("a")) {
                    assertThat(c2).isEqualTo("a,b,c");
                } else if (c1.equals("b")) {
                    assertThat(c2).isEqualTo("a,b");
                } else if (c1.equals("c")) {
                    assertThat(c2).isEqualTo("a");
                } else {
                    fail("c1 didn't match expected value");
                }
            } else if (record.topic().endsWith("dbz_102_charsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String text = after.getString("text");
                assertThat(text).isEqualTo("产品");
            } else if (record.topic().endsWith("dbz_85_fractest")) {
                // The microseconds of all three should be exactly 780
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 DATE,
                // c2 TIME(2),
                // c3 DATETIME(2),
                // c4 TIMESTAMP(2)
                //
                // {"c1" : "16321", "c2" : "64264780", "c3" : "1410198664780", "c4" : "2014-09-08T17:51:04.78-05:00"}

                // '2014-09-08'
                java.util.Date c1 = (java.util.Date) after.get("c1"); // epoch days
                LocalDate c1Date = LocalDate.ofEpochDay(c1.getTime() / TimeUnit.DAYS.toMillis(1));
                assertThat(c1Date.getYear()).isEqualTo(2014);
                assertThat(c1Date.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c1Date.getDayOfMonth()).isEqualTo(8);

                // '17:51:04.777'
                java.util.Date c2 = (java.util.Date) after.get("c2"); // milliseconds past midnight
                LocalTime c2Time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(c2.getTime()));
                assertThat(c2Time.getHour()).isEqualTo(17);
                assertThat(c2Time.getMinute()).isEqualTo(51);
                assertThat(c2Time.getSecond()).isEqualTo(4);
                assertThat(c2Time.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time)).isEqualTo((int) c2.getTime());

                // '2014-09-08 17:51:04.777'
                java.util.Date c3 = (java.util.Date) after.get("c3"); // epoch millis
                long c3Seconds = c3.getTime() / 1000;
                long c3Millis = c3.getTime() % 1000;
                LocalDateTime c3DateTime = LocalDateTime.ofEpochSecond(c3Seconds,
                                                                       (int) TimeUnit.MILLISECONDS.toNanos(c3Millis),
                                                                       ZoneOffset.UTC);
                assertThat(c3DateTime.getYear()).isEqualTo(2014);
                assertThat(c3DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c3DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c3DateTime.getHour()).isEqualTo(17);
                assertThat(c3DateTime.getMinute()).isEqualTo(51);
                assertThat(c3DateTime.getSecond()).isEqualTo(4);
                assertThat(c3DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime)).isEqualTo(c3.getTime());

                // '2014-09-08 17:51:04.777'
                String c4 = after.getString("c4"); // MySQL timestamp, so always ZonedTimestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(OffsetDateTime.now().getOffset());
                assertThat(c4DateTime.getYear()).isEqualTo(2014);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c4DateTime.getHour()).isEqualTo(17);
                assertThat(c4DateTime.getMinute()).isEqualTo(51);
                assertThat(c4DateTime.getSecond()).isEqualTo(4);
                assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            }
        });
    }

    @Test
    public void shouldConsumeAllEventsFromDatabaseUsingSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                              .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname"))
                              .with(MySqlConnectorConfig.PORT, System.getProperty("database.port"))
                              .with(MySqlConnectorConfig.USER, "snapper")
                              .with(MySqlConnectorConfig.PASSWORD, "snapperpass")
                              .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                              .with(MySqlConnectorConfig.SERVER_ID, 18765)
                              .with(MySqlConnectorConfig.SERVER_NAME, "regression")
                              .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                              .with(MySqlConnectorConfig.DATABASE_WHITELIST, "regression_test")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.toString())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        Testing.Debug.enable();
        // We expect a total of 14 schema change records:
        // 1 set variables
        // 6 drop tables
        // 1 drop database
        // 1 create database
        // 1 use database
        // 6 create tables
        SourceRecords records = consumeRecordsByTopic(14 + 7); // plus 7 data records ...
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(14);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(6);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.databaseNames()).containsOnly("regression_test","");
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(13);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase("regression_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_100_enumsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String c1 = after.getString("c1");
                String c2 = after.getString("c2");
                if (c1.equals("a")) {
                    assertThat(c2).isEqualTo("a,b,c");
                } else if (c1.equals("b")) {
                    assertThat(c2).isEqualTo("a,b");
                } else if (c1.equals("c")) {
                    assertThat(c2).isEqualTo("a");
                } else {
                    fail("c1 didn't match expected value");
                }
            } else if (record.topic().endsWith("dbz_102_charsettest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                String text = after.getString("text");
                assertThat(text).isEqualTo("产品");
            } else if (record.topic().endsWith("dbz_85_fractest")) {
                // The microseconds of all three should be exactly 780
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 DATE,
                // c2 TIME(2),
                // c3 DATETIME(2),
                // c4 TIMESTAMP(2)
                //
                // {"c1" : "16321", "c2" : "64264780", "c3" : "1410198664780", "c4" : "2014-09-08T17:51:04.78-05:00"}

                // '2014-09-08'
                Integer c1 = after.getInt32("c1"); // epoch days
                LocalDate c1Date = LocalDate.ofEpochDay(c1);
                assertThat(c1Date.getYear()).isEqualTo(2014);
                assertThat(c1Date.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c1Date.getDayOfMonth()).isEqualTo(8);
                assertThat(io.debezium.time.Date.toEpochDay(c1Date)).isEqualTo(c1);

                // '17:51:04.777'
                Integer c2 = after.getInt32("c2"); // milliseconds past midnight
                LocalTime c2Time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(c2));
                assertThat(c2Time.getHour()).isEqualTo(17);
                assertThat(c2Time.getMinute()).isEqualTo(51);
                assertThat(c2Time.getSecond()).isEqualTo(4);
                assertThat(c2Time.getNano()).isEqualTo(0); // What!?!? The MySQL Connect/J driver indeed returns 0 for fractional
                                                           // part
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time)).isEqualTo(c2);

                // '2014-09-08 17:51:04.777'
                Long c3 = after.getInt64("c3"); // epoch millis
                long c3Seconds = c3 / 1000;
                long c3Millis = c3 % 1000;
                LocalDateTime c3DateTime = LocalDateTime.ofEpochSecond(c3Seconds,
                                                                       (int) TimeUnit.MILLISECONDS.toNanos(c3Millis),
                                                                       ZoneOffset.UTC);
                assertThat(c3DateTime.getYear()).isEqualTo(2014);
                assertThat(c3DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c3DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c3DateTime.getHour()).isEqualTo(17);
                assertThat(c3DateTime.getMinute()).isEqualTo(51);
                assertThat(c3DateTime.getSecond()).isEqualTo(4);
                assertThat(c3DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime)).isEqualTo(c3);

                // '2014-09-08 17:51:04.777'
                String c4 = after.getString("c4"); // timestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(OffsetDateTime.now().getOffset());
                assertThat(c4DateTime.getYear()).isEqualTo(2014);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(8);
                assertThat(c4DateTime.getHour()).isEqualTo(17);
                assertThat(c4DateTime.getMinute()).isEqualTo(51);
                assertThat(c4DateTime.getSecond()).isEqualTo(4);
                assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            }
        });
    }

}

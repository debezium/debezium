/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.fest.assertions.Delta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.DecimalHandlingMode;
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
        int numCreateDatabase = 1;
        int numCreateTables = 9;
        int numDataRecords = 16;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_114_zerovaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_123_bitvaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_104_customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_147_decimalvalues").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(numCreateDatabase + numCreateTables);
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
                assertTimestamp(c4);
            } else if (record.topic().endsWith("dbz_114_zerovaluetest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 DATE,
                // c2 TIME(2),
                // c3 DATETIME(2),
                // c4 TIMESTAMP(2)
                //
                // INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0000-00-00', '00:00:00.000', '0000-00-00 00:00:00.000',
                // '0000-00-00 00:00:00.000');
                // INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0001-00-00', '00:01:00.000', '0001-00-00 00:00:00.000',
                // '0001-00-00 00:00:00.000');
                //
                // results in:
                //
                // +------------+-------------+------------------------+------------------------+
                // | c1 | c2 | c3 | c4 |
                // +------------+-------------+------------------------+------------------------+
                // | 0000-00-00 | 00:00:00.00 | 0000-00-00 00:00:00.00 | 0000-00-00 00:00:00.00 |
                // | 0000-00-00 | 00:01:00.00 | 0000-00-00 00:00:00.00 | 0000-00-00 00:00:00.00 |
                // +------------+-------------+------------------------+------------------------+
                //
                // Note the '0001' years that were inserted are stored as '0000'
                //
                assertThat(after.getInt32("c1")).isNull(); // epoch days

                // '00:00:00.000'
                Integer c2 = after.getInt32("c2"); // milliseconds past midnight
                LocalTime c2Time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(c2));
                assertThat(c2Time.getHour()).isEqualTo(0);
                assertThat(c2Time.getMinute() == 0 || c2Time.getMinute() == 1).isTrue();
                assertThat(c2Time.getSecond()).isEqualTo(0);
                assertThat(c2Time.getNano()).isEqualTo(0);
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time)).isEqualTo(c2);

                assertThat(after.getInt64("c3")).isNull(); // epoch millis

                // '0000-00-00 00:00:00.00'
                String c4 = after.getString("c4"); // timestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(OffsetDateTime.now().getOffset());
                assertThat(c4DateTime.getYear()).isEqualTo(1970);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.JANUARY);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(1);
                assertThat(c4DateTime.getHour()).isEqualTo(0);
                assertThat(c4DateTime.getMinute()).isEqualTo(0);
                assertThat(c4DateTime.getSecond()).isEqualTo(0);
                assertThat(c4DateTime.getNano()).isEqualTo(0);
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            } else if (record.topic().endsWith("dbz_123_bitvaluetest")) {
                // All row events should have the same values ...
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 BIT, // 1 bit
                // c2 BIT(2), // 2 bits
                // c3 BIT(8) // 8 bits
                // c4 BIT(64) // 64 bits
                Boolean c1 = after.getBoolean("c1");
                assertThat(c1).isEqualTo(Boolean.TRUE);

                byte[] c2 = after.getBytes("c2");
                assertThat(c2.length).isEqualTo(1);
                assertThat(c2[0]).isEqualTo((byte) 2);

                byte[] c3 = after.getBytes("c3");
                assertThat(c3.length).isEqualTo(1);
                assertThat(c3[0]).isEqualTo((byte) 64);

                // 1011011100000111011011011 = 23989979
                byte[] c4 = after.getBytes("c4");
                assertThat(c4.length).isEqualTo(8); // bytes, little endian
                assertThat(c4[0]).isEqualTo((byte) 219); // 11011011
                assertThat(c4[1]).isEqualTo((byte) 14); // 00001110
                assertThat(c4[2]).isEqualTo((byte) 110); // 01101110
                assertThat(c4[3]).isEqualTo((byte) 1); // 1
                assertThat(c4[4]).isEqualTo((byte) 0);
                assertThat(c4[5]).isEqualTo((byte) 0);
                assertThat(c4[6]).isEqualTo((byte) 0);
                assertThat(c4[7]).isEqualTo((byte) 0);
            } else if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(BigDecimal.class);
                BigDecimal bigValue = (BigDecimal) decimalValue;
                assertThat(bigValue.doubleValue()).isEqualTo(12345.67, Delta.delta(0.01));
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
        int numCreateDatabase = 1;
        int numCreateTables = 9;
        int numDataRecords = 16;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_114_zerovaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_123_bitvaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_104_customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_147_decimalvalues").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase("regression_test").forEach(this::print);

        // Check that all records are valid, can be serialized and deserialized ...
        // records.forEach(this::validate); // Can't run this with 0.10.0.1; see KAFKA-4183
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
                assertTimestamp(c4);
            } else if (record.topic().endsWith("dbz_114_zerovaluetest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 DATE,
                // c2 TIME(2),
                // c3 DATETIME(2),
                // c4 TIMESTAMP(2)
                //
                // INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0000-00-00', '00:00:00.000', '0000-00-00 00:00:00.000',
                // '0000-00-00 00:00:00.000');
                // INSERT IGNORE INTO dbz_114_zerovaluetest VALUES ('0001-00-00', '00:01:00.000', '0001-00-00 00:00:00.000',
                // '0001-00-00 00:00:00.000');
                //
                // results in:
                //
                // +------------+-------------+------------------------+------------------------+
                // | c1 | c2 | c3 | c4 |
                // +------------+-------------+------------------------+------------------------+
                // | 0000-00-00 | 00:00:00.00 | 0000-00-00 00:00:00.00 | 0000-00-00 00:00:00.00 |
                // | 0000-00-00 | 00:01:00.00 | 0000-00-00 00:00:00.00 | 0000-00-00 00:00:00.00 |
                // +------------+-------------+------------------------+------------------------+
                //
                // Note the '0001' years that were inserted are stored as '0000'
                //
                java.util.Date c1 = (java.util.Date) after.get("c1"); // epoch days
                assertThat(c1).isNull();

                // '00:00:00.000'
                java.util.Date c2 = (java.util.Date) after.get("c2"); // milliseconds past midnight
                LocalTime c2Time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(c2.getTime()));
                assertThat(c2Time.getHour()).isEqualTo(0);
                assertThat(c2Time.getMinute() == 0 || c2Time.getMinute() == 1).isTrue();
                assertThat(c2Time.getSecond()).isEqualTo(0);
                assertThat(c2Time.getNano()).isEqualTo(0);
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time)).isEqualTo((int) c2.getTime());

                java.util.Date c3 = (java.util.Date) after.get("c3"); // epoch millis
                assertThat(c3).isNull();

                // '0000-00-00 00:00:00.00'
                String c4 = after.getString("c4"); // MySQL timestamp, so always ZonedTimestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(OffsetDateTime.now().getOffset());
                assertThat(c4DateTime.getYear()).isEqualTo(1970);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.JANUARY);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(1);
                assertThat(c4DateTime.getHour()).isEqualTo(0);
                assertThat(c4DateTime.getMinute()).isEqualTo(0);
                assertThat(c4DateTime.getSecond()).isEqualTo(0);
                assertThat(c4DateTime.getNano()).isEqualTo(0);
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            } else if (record.topic().endsWith("dbz_123_bitvaluetest")) {
                // All row events should have the same values ...
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 BIT, // 1 bit
                // c2 BIT(2), // 2 bits
                // c3 BIT(8) // 8 bits
                // c4 BIT(64) // 64 bits
                Boolean c1 = after.getBoolean("c1");
                assertThat(c1).isEqualTo(Boolean.TRUE);

                byte[] c2 = after.getBytes("c2");
                assertThat(c2.length).isEqualTo(1);
                assertThat(c2[0]).isEqualTo((byte) 2);

                byte[] c3 = after.getBytes("c3");
                assertThat(c3.length).isEqualTo(1);
                assertThat(c3[0]).isEqualTo((byte) 64);

                // 1011011100000111011011011 = 23989979
                byte[] c4 = after.getBytes("c4");
                assertThat(c4.length).isEqualTo(8); // bytes, little endian
                assertThat(c4[0]).isEqualTo((byte) 219); // 11011011
                assertThat(c4[1]).isEqualTo((byte) 14); // 00001110
                assertThat(c4[2]).isEqualTo((byte) 110); // 01101110
                assertThat(c4[3]).isEqualTo((byte) 1); // 1
                assertThat(c4[4]).isEqualTo((byte) 0);
                assertThat(c4[5]).isEqualTo((byte) 0);
                assertThat(c4[6]).isEqualTo((byte) 0);
                assertThat(c4[7]).isEqualTo((byte) 0);
            } else if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(BigDecimal.class);
                BigDecimal bigValue = (BigDecimal) decimalValue;
                assertThat(bigValue.doubleValue()).isEqualTo(12345.67, Delta.delta(0.01));
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
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numTables = 9;
        int numDataRecords = 16;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numSetVariables = 1;
        SourceRecords records = consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic("regression.regression_test.t1464075356413_testtable6").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz84_integer_types_table").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_85_fractest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_100_enumsettest").size()).isEqualTo(3);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_102_charsettest").size()).isEqualTo(1);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_114_zerovaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_123_bitvaluetest").size()).isEqualTo(2);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_104_customers").size()).isEqualTo(4);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_147_decimalvalues").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.databaseNames()).containsOnly("regression_test", "");
        assertThat(records.ddlRecordsForDatabase("regression_test").size()).isEqualTo(numDdlRecords);
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
                // Difference depends upon whether the zone we're in is also using DST as it is on the date in question ...
                assertThat(c4DateTime.getHour() == 16 || c4DateTime.getHour() == 17).isTrue();
                assertThat(c4DateTime.getMinute()).isEqualTo(51);
                assertThat(c4DateTime.getSecond()).isEqualTo(4);
                assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current offset ...
                assertThat(c4DateTime.getOffset()).isEqualTo(OffsetDateTime.now().getOffset());
            } else if (record.topic().endsWith("dbz_123_bitvaluetest")) {
                // All row events should have the same values ...
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c1 BIT, // 1 bit
                // c2 BIT(2), // 2 bits
                // c3 BIT(8) // 8 bits
                // c4 BIT(64) // 64 bits
                Boolean c1 = after.getBoolean("c1");
                assertThat(c1).isEqualTo(Boolean.TRUE);

                byte[] c2 = after.getBytes("c2");
                assertThat(c2.length).isEqualTo(1);
                assertThat(c2[0]).isEqualTo((byte) 2);

                byte[] c3 = after.getBytes("c3");
                assertThat(c3.length).isEqualTo(1);
                assertThat(c3[0]).isEqualTo((byte) 64);

                // 1011011100000111011011011 = 23989979
                byte[] c4 = after.getBytes("c4");
                assertThat(c4.length).isEqualTo(8); // bytes, little endian
                assertThat(c4[0]).isEqualTo((byte) 219); // 11011011
                assertThat(c4[1]).isEqualTo((byte) 14); // 00001110
                assertThat(c4[2]).isEqualTo((byte) 110); // 01101110
                assertThat(c4[3]).isEqualTo((byte) 1); // 1
                assertThat(c4[4]).isEqualTo((byte) 0);
                assertThat(c4[5]).isEqualTo((byte) 0);
                assertThat(c4[6]).isEqualTo((byte) 0);
                assertThat(c4[7]).isEqualTo((byte) 0);
            } else if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(BigDecimal.class);
                BigDecimal bigValue = (BigDecimal) decimalValue;
                assertThat(bigValue.doubleValue()).isEqualTo(12345.67, Delta.delta(0.01));
            }
        });
    }

    @Test
    @FixFor("DBZ-147")
    public void shouldConsumeAllEventsFromDecimalTableInDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
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
                              .with(MySqlConnectorConfig.TABLE_WHITELIST, "regression_test.dbz_147_decimalvalues")
                              .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE.name())
                              .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        //Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 9; // still read DDL for all tables
        int numDataRecords = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic("regression").size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic("regression.regression_test.dbz_147_decimalvalues").size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(2); // rather than 1+numCreateTables

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(Double.class);
                Double doubleValue = (Double) decimalValue;
                assertThat(doubleValue).isEqualTo(12345.67, Delta.delta(0.01));
            }
        });
    }
    
    private void assertTimestamp(String c4) {
        // '2014-09-08 17:51:04.777'
        ZoneId defaultZoneId = ZoneId.systemDefault();
        ZonedDateTime c4DateTime = ZonedDateTime.parse(c4, ZonedTimestamp.FORMATTER).withZoneSameInstant(defaultZoneId);
        assertThat(c4DateTime.getYear()).isEqualTo(2014);
        assertThat(c4DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
        assertThat(c4DateTime.getDayOfMonth()).isEqualTo(8);
        assertThat(c4DateTime.getHour()).isEqualTo(17);
        assertThat(c4DateTime.getMinute()).isEqualTo(51);
        assertThat(c4DateTime.getSecond()).isEqualTo(4);
        assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
        // We're running the connector in the same timezone as the server, so the timezone in the timestamp
        // should match our current offset ...
        LocalDateTime expectedLocalDateTime = LocalDateTime.parse("2014-09-08T17:51:04.780");
        ZoneOffset expectedOffset = defaultZoneId.getRules().getOffset(expectedLocalDateTime);
        assertThat(c4DateTime.getOffset()).isEqualTo(expectedOffset);
    }
}

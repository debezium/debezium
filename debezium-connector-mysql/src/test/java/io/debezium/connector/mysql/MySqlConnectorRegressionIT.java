/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjuster;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.fest.assertions.Delta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.DecimalHandlingMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class MySqlConnectorRegressionIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-regression.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("regression", "regression_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private static final TemporalAdjuster ADJUSTER = MySqlValueConverters::adjustTemporal;

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
        } finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-61")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 11;
        int numDataRecords = 20;
        int numCreateDefiner = 1;
        SourceRecords records =
            consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords + numCreateDefiner);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size())
            .isEqualTo(numCreateDatabase + numCreateTables + numCreateDefiner);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("t1464075356413_testtable6")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz84_integer_types_table")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_85_fractest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_100_enumsettest")).size()).isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_102_charsettest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_114_zerovaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_123_bitvaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_104_customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_342_timetest")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(numCreateTables + 1);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size())
            .isEqualTo(numCreateDatabase + numCreateTables + numCreateDefiner);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

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
                // {"c1" : "16321", "c2" : "17:51:04.777", "c3" : "1410198664780", "c4" : "2014-09-08T17:51:04.78-05:00"}

                // '2014-09-08'
                Integer c1 = after.getInt32("c1"); // epoch days
                LocalDate c1Date = LocalDate.ofEpochDay(c1);
                assertThat(c1Date.getYear()).isEqualTo(2014);
                assertThat(c1Date.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c1Date.getDayOfMonth()).isEqualTo(8);
                assertThat(io.debezium.time.Date.toEpochDay(c1Date, ADJUSTER)).isEqualTo(c1);

                // '17:51:04.777'
                Long c2 = after.getInt64("c2");
                Duration c2Time = Duration.ofNanos(c2 * 1_000);
                assertThat(c2Time.toHours()).isEqualTo(17);
                assertThat(c2Time.toMinutes()).isEqualTo(1071);
                assertThat(c2Time.getSeconds()).isEqualTo(64264);
                assertThat(c2Time.getNano()).isEqualTo(780000000);
                assertThat(c2Time.toNanos()).isEqualTo(64264780000000L);
                assertThat(c2Time).isEqualTo(Duration.ofHours(17).plusMinutes(51).plusSeconds(4).plusMillis(780));

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
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime, ADJUSTER)).isEqualTo(c3);

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
                Long c2 = after.getInt64("c2");
                Duration c2Time = Duration.ofNanos(c2 * 1_000);
                assertThat(c2Time.toHours()).isEqualTo(0);
                assertThat(c2Time.toMinutes() == 1 || c2Time.toMinutes() == 0).isTrue();
                assertThat(c2Time.getSeconds() == 0 || c2Time.getSeconds() == 60).isTrue();
                assertThat(c2Time.getNano()).isEqualTo(0);
                assertThat(c2Time.toNanos() == 0 || c2Time.toNanos() == 60000000000L).isTrue();
                assertThat(c2Time.equals(Duration.ofSeconds(0)) || c2Time.equals(Duration.ofMinutes(1))).isTrue();

                assertThat(after.getInt64("c3")).isNull(); // epoch millis

                // '0000-00-00 00:00:00.00'
                String c4 = after.getString("c4"); // timestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);

                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our current TZ's offset at the given time...
                ZoneOffset expectedOffset = ZonedDateTime.of(
                        LocalDate.of(1970, 1, 1).atTime(0, 0),
                        TimeZone.getDefault().toZoneId()
                ).getOffset();
                assertThat(c4DateTime.getOffset()).isEqualTo(expectedOffset);

                // In case the timestamp string not in our timezone, convert to UTC so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(ZoneOffset.of("Z"));
                assertThat(c4DateTime.getYear()).isEqualTo(1970);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.JANUARY);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(1);
                // Difference depends upon whether the zone we're in is also using DST as it is on the date in question ...
                assertThat(c4DateTime.getHour()).isIn(0, 1);
                assertThat(c4DateTime.getMinute()).isEqualTo(0);
                assertThat(c4DateTime.getSecond()).isEqualTo(0);
                assertThat(c4DateTime.getNano()).isEqualTo(0);
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
            } else if (record.topic().endsWith("dbz_342_timetest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);

                // '517:51:04.777'
                long c1 = after.getInt64("c1");
                Duration c1Time = Duration.ofNanos(c1 * 1_000);
                Duration c1ExpectedTime = toDuration("PT517H51M4.78S");
                assertEquals(c1ExpectedTime, c1Time);
                assertEquals(c1ExpectedTime.toNanos(), c1Time.toNanos());
                assertThat(c1Time.toNanos()).isEqualTo(1864264780000000L);
                assertThat(c1Time).isEqualTo(Duration.ofHours(517).plusMinutes(51).plusSeconds(4).plusMillis(780));

                // '-13:14:50'
                long c2 = after.getInt64("c2");
                Duration c2Time = Duration.ofNanos(c2 * 1_000);
                Duration c2ExpectedTime = toDuration("-PT13H14M50S");
                assertEquals(c2ExpectedTime, c2Time);
                assertEquals(c2ExpectedTime.toNanos(), c2Time.toNanos());
                assertThat(c2Time.toNanos()).isEqualTo(-47690000000000L);
                assertTrue(c2Time.isNegative());
                assertThat(c2Time).isEqualTo(Duration.ofHours(-13).minusMinutes(14).minusSeconds(50));

                // '-733:00:00.0011'
                long c3 = after.getInt64("c3");
                Duration c3Time = Duration.ofNanos(c3 * 1_000);
                Duration c3ExpectedTime = toDuration("-PT733H0M0.001S");
                assertEquals(c3ExpectedTime, c3Time);
                assertEquals(c3ExpectedTime.toNanos(), c3Time.toNanos());
                assertThat(c3Time.toNanos()).isEqualTo(-2638800001000000L);
                assertTrue(c3Time.isNegative());
                assertThat(c3Time).isEqualTo(Duration.ofHours(-733).minusMillis(1));

                // '-1:59:59.0011'
                long c4 = after.getInt64("c4");
                Duration c4Time = Duration.ofNanos(c4 * 1_000);
                Duration c4ExpectedTime = toDuration("-PT1H59M59.001S");
                assertEquals(c4ExpectedTime, c4Time);
                assertEquals(c4ExpectedTime.toNanos(), c4Time.toNanos());
                assertThat(c4Time.toNanos()).isEqualTo(-7199001000000L);
                assertTrue(c4Time.isNegative());
                assertThat(c4Time).isEqualTo(Duration.ofHours(-1).minusMinutes(59).minusSeconds(59).minusMillis(1));

                // '-838:59:58.999999'
                long c5 = after.getInt64("c5");
                Duration c5Time = Duration.ofNanos(c5 * 1_000);
                Duration c5ExpectedTime = toDuration("-PT838H59M58.999999S");
                assertEquals(c5ExpectedTime, c5Time);
                assertEquals(c5ExpectedTime.toNanos(), c5Time.toNanos());
                assertThat(c5Time.toNanos()).isEqualTo(-3020398999999000L);
                assertTrue(c5Time.isNegative());
                assertThat(c5Time).isEqualTo(Duration.ofHours(-838).minusMinutes(59).minusSeconds(58).minusNanos(999999000));
            }
        });
    }

    @Test
    @FixFor("DBZ-61")
    public void shouldConsumeAllEventsFromDatabaseUsingBinlogAndNoSnapshotAndConnectTimesTypes() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER)
                              .with(MySqlConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 11;
        int numDataRecords = 20;
        int numCreateDefiner = 1;
        SourceRecords records =
                consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords + numCreateDefiner);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size())
            .isEqualTo(numCreateDatabase + numCreateTables + numCreateDefiner);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("t1464075356413_testtable6")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz84_integer_types_table")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_85_fractest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_100_enumsettest")).size()).isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_102_charsettest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_114_zerovaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_123_bitvaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_104_customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(1 + numCreateTables);
        assertThat(records.databaseNames().size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size())
            .isEqualTo(numCreateDatabase + numCreateTables + numCreateDefiner);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

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
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time, ADJUSTER)).isEqualTo((int) c2.getTime());

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
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime, ADJUSTER)).isEqualTo(c3.getTime());

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
                assertThat(io.debezium.time.Time.toMilliOfDay(c2Time, ADJUSTER)).isEqualTo((int) c2.getTime());

                java.util.Date c3 = (java.util.Date) after.get("c3"); // epoch millis
                assertThat(c3).isNull();

                // '0000-00-00 00:00:00.00'
                String c4 = after.getString("c4"); // MySQL timestamp, so always ZonedTimestamp
                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);

                // We're running the connector in the same timezone as the server, so the timezone in the timestamp
                // should match our TZ's offset at the given time...
                ZoneOffset expectedOffset = ZonedDateTime.of(
                        LocalDate.of(1970, 1, 1).atTime(0, 0),
                        TimeZone.getDefault().toZoneId()
                ).getOffset();
                assertThat(c4DateTime.getOffset()).isEqualTo(expectedOffset);

                // In case the timestamp string not in our timezone, convert to UTC so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(ZoneOffset.of("Z"));
                assertThat(c4DateTime.getYear()).isEqualTo(1970);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.JANUARY);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(1);
                // Difference depends upon whether the zone we're in is also using DST as it is on the date in question ...
                assertThat(c4DateTime.getHour()).isIn(0, 1);
                assertThat(c4DateTime.getMinute()).isEqualTo(0);
                assertThat(c4DateTime.getSecond()).isEqualTo(0);
                assertThat(c4DateTime.getNano()).isEqualTo(0);
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
        config = DATABASE.defaultConfig().build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numTables = 11;
        int numDataRecords = 20;
        int numDdlRecords = numTables * 2 + 3; // for each table (1 drop + 1 create) + for each db (1 create + 1 drop + 1 use)
        int numCreateDefiner = 1;
        int numSetVariables = 1;
        SourceRecords records =
            consumeRecordsByTopic(numDdlRecords + numSetVariables + numDataRecords + numCreateDefiner + numCreateDatabase);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size())
            .isEqualTo(numDdlRecords + numSetVariables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("t1464075356413_testtable6")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz84_integer_types_table")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_85_fractest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_100_enumsettest")).size()).isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_102_charsettest")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_114_zerovaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_123_bitvaluetest")).size()).isEqualTo(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_104_customers")).size()).isEqualTo(4);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_195_numvalues")).size()).isEqualTo(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_342_timetest")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(numTables + 1);
        assertThat(records.databaseNames().size()).isEqualTo(2);
        assertThat(records.databaseNames()).containsOnly(DATABASE.getDatabaseName(), "");
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(numDdlRecords);
        assertThat(records.ddlRecordsForDatabase("connector_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("readbinlog_test")).isNull();
        assertThat(records.ddlRecordsForDatabase("").size()).isEqualTo(1); // SET statement
        records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).forEach(this::print);

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
                // {"c1" : "16321", "c2" : "17:51:04.777", "c3" : "1410198664780", "c4" : "2014-09-08T17:51:04.78-05:00"}

                // '2014-09-08'
                Integer c1 = after.getInt32("c1"); // epoch days
                LocalDate c1Date = LocalDate.ofEpochDay(c1);
                assertThat(c1Date.getYear()).isEqualTo(2014);
                assertThat(c1Date.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c1Date.getDayOfMonth()).isEqualTo(8);
                assertThat(io.debezium.time.Date.toEpochDay(c1Date, ADJUSTER)).isEqualTo(c1);

                // '17:51:04.777'
                Long c2 = after.getInt64("c2");
                Duration c2Time = Duration.ofNanos(c2 * 1_000);
                assertThat(c2Time.toHours()).isEqualTo(17);
                assertThat(c2Time.toMinutes()).isEqualTo(1071);
                assertThat(c2Time.getSeconds()).isEqualTo(64264);
                assertThat(c2Time.getNano()).isEqualTo(780000000);
                assertThat(c2Time.toNanos()).isEqualTo(64264780000000L);
                assertThat(c2Time).isEqualTo(Duration.ofHours(17).plusMinutes(51).plusSeconds(4).plusMillis(780));

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
                assertThat(io.debezium.time.Timestamp.toEpochMillis(c3DateTime, ADJUSTER)).isEqualTo(c3);

                // '2014-09-08 17:51:04.777'
                String c4 = after.getString("c4"); // timestamp

                ZoneOffset localOffset = ZonedDateTime.of(
                        LocalDate.of(2014, 9, 8).atTime(0, 0),
                        TimeZone.getDefault().toZoneId()
                ).getOffset();

                OffsetDateTime c4DateTime = OffsetDateTime.parse(c4, ZonedTimestamp.FORMATTER);
                // In case the timestamp string not in our timezone, convert to ours so we can compare ...
                c4DateTime = c4DateTime.withOffsetSameInstant(localOffset);
                assertThat(c4DateTime.getYear()).isEqualTo(2014);
                assertThat(c4DateTime.getMonth()).isEqualTo(Month.SEPTEMBER);
                assertThat(c4DateTime.getDayOfMonth()).isEqualTo(8);
                // Difference depends upon whether the zone we're in is also using DST as it is on the date in question ...
                assertThat(c4DateTime.getHour() == 16 || c4DateTime.getHour() == 17).isTrue();
                assertThat(c4DateTime.getMinute()).isEqualTo(51);
                assertThat(c4DateTime.getSecond()).isEqualTo(4);
                assertThat(c4DateTime.getNano()).isEqualTo((int) TimeUnit.MILLISECONDS.toNanos(780));
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
            } else if (record.topic().endsWith("dbz_195_numvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object searchVersion = after.get("search_version_read");
                assertThat(searchVersion).isInstanceOf(Integer.class);
                Integer intValue = (Integer) searchVersion;
                if (intValue.intValue() < 0) {
                    assertThat(intValue.intValue()).isEqualTo(-2147483648);
                } else if (intValue.intValue() > 0) {
                    assertThat(intValue.intValue()).isEqualTo(2147483647);
                } else {
                    assertThat(intValue.intValue()).isEqualTo(0);
                }
            }  else if (record.topic().endsWith("dbz_342_timetest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);

                // '517:51:04.777'
                long c1 = after.getInt64("c1");
                Duration c1Time = Duration.ofNanos(c1 * 1_000);
                Duration c1ExpectedTime = toDuration("PT517H51M4.78S");
                assertEquals(c1ExpectedTime, c1Time);
                assertEquals(c1ExpectedTime.toNanos(), c1Time.toNanos());
                assertThat(c1Time.toNanos()).isEqualTo(1864264780000000L);
                assertThat(c1Time).isEqualTo(Duration.ofHours(517).plusMinutes(51).plusSeconds(4).plusMillis(780));

                // '-13:14:50'
                long c2 = after.getInt64("c2");
                Duration c2Time = Duration.ofNanos(c2 * 1_000);
                Duration c2ExpectedTime = toDuration("-PT13H14M50S");
                assertEquals(c2ExpectedTime, c2Time);
                assertEquals(c2ExpectedTime.toNanos(), c2Time.toNanos());
                assertThat(c2Time.toNanos()).isEqualTo(-47690000000000L);
                assertTrue(c2Time.isNegative());
                assertThat(c2Time).isEqualTo(Duration.ofHours(-13).minusMinutes(14).minusSeconds(50));

                // '-733:00:00.0011'
                long c3 = after.getInt64("c3");
                Duration c3Time = Duration.ofNanos(c3 * 1_000);
                Duration c3ExpectedTime = toDuration("-PT733H0M0.001S");
                assertEquals(c3ExpectedTime, c3Time);
                assertEquals(c3ExpectedTime.toNanos(), c3Time.toNanos());
                assertThat(c3Time.toNanos()).isEqualTo(-2638800001000000L);
                assertTrue(c3Time.isNegative());
                assertThat(c3Time).isEqualTo(Duration.ofHours(-733).minusMillis(1));

                // '-1:59:59.0011'
                long c4 = after.getInt64("c4");
                Duration c4Time = Duration.ofNanos(c4 * 1_000);
                Duration c4ExpectedTime = toDuration("-PT1H59M59.001S");
                assertEquals(c4ExpectedTime, c4Time);
                assertEquals(c4ExpectedTime.toNanos(), c4Time.toNanos());
                assertThat(c4Time.toNanos()).isEqualTo(-7199001000000L);
                assertTrue(c4Time.isNegative());
                assertThat(c4Time).isEqualTo(Duration.ofHours(-1).minusMinutes(59).minusSeconds(59).minusMillis(1));

                // '-838:59:58.999999'
                long c5 = after.getInt64("c5");
                Duration c5Time = Duration.ofNanos(c5 * 1_000);
                Duration c5ExpectedTime = toDuration("-PT838H59M58.999999S");
                assertEquals(c5ExpectedTime, c5Time);
                assertEquals(c5ExpectedTime.toNanos(), c5Time.toNanos());
                assertThat(c5Time.toNanos()).isEqualTo(-3020398999999000L);
                assertTrue(c5Time.isNegative());
                assertThat(c5Time).isEqualTo(Duration.ofHours(-838).minusMinutes(59).minusSeconds(58).minusNanos(999999000));
            }
        });
    }

    @Test
    @FixFor("DBZ-147")
    public void shouldConsumeAllEventsFromDecimalTableInDatabaseUsingBinlogAndNoSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("dbz_147_decimalvalues"))
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.DOUBLE)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 9; // still read DDL for all tables
        int numDataRecords = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
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

    @Test
    @FixFor("DBZ-611")
    public void shouldConsumeDecimalAsStringFromBinlog() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("dbz_147_decimalvalues"))
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.toString())
                              .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 9; // still read DDL for all tables
        int numDataRecords = 1;
        SourceRecords records = consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(2); // rather than 1+numCreateTables

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(String.class);
                assertThat(decimalValue).isEqualTo("12345.67");
            }
        });
    }

    @Test
    @FixFor("DBZ-611")
    public void shouldConsumeDecimalAsStringFromSnapshot() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                              .with(MySqlConnectorConfig.TABLE_WHITELIST, DATABASE.qualifiedTableName("dbz_147_decimalvalues"))
                              .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                              .with(MySqlConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING)
                              .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int ddlRecords = 6;
        int numDataRecords = 1;
        SourceRecords records = consumeRecordsByTopic(ddlRecords + numDataRecords);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(ddlRecords);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_147_decimalvalues")).size()).isEqualTo(1);
        assertThat(records.topics().size()).isEqualTo(2); // rather than 1+numCreateTables

        // Check that all records are valid, can be serialized and deserialized ...
        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_147_decimalvalues")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                Object decimalValue = after.get("decimal_value");
                assertThat(decimalValue).isInstanceOf(String.class);
                assertThat(decimalValue).isEqualTo("12345.67");
            }
        });
    }
    @Test
    @FixFor("DBZ-342")
    public void shouldReturnTimeColumnsAsMilliSecondsInAdaptivePrecisionMode() throws SQLException, InterruptedException {
        // Use the DB configuration to define the connector's configuration ...
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .with(MySqlConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE)
                .build();
        // Start the connector ...
        start(MySqlConnector.class, config);

        // ---------------------------------------------------------------------------------------------------------------
        // Consume all of the events due to startup and initialization of the database
        // ---------------------------------------------------------------------------------------------------------------
        // Testing.Debug.enable();
        int numCreateDatabase = 1;
        int numCreateTables = 11;
        int numDataRecords = 20;
        int numCreateDefiner = 1;
        SourceRecords records =
            consumeRecordsByTopic(numCreateDatabase + numCreateTables + numDataRecords + numCreateDefiner);
        stopConnector();
        assertThat(records).isNotNull();
        assertThat(records.recordsForTopic(DATABASE.getServerName()).size()).isEqualTo(numCreateDatabase + numCreateTables + numCreateDefiner);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("dbz_85_fractest")).size()).isEqualTo(1);

        records.forEach(this::validate);
        records.forEach(record -> {
            Struct value = (Struct) record.value();
            if (record.topic().endsWith("dbz_85_fractest")) {
                Struct after = value.getStruct(Envelope.FieldName.AFTER);
                // c2 TIME(2),
                // { "c2" : "17:51:04.777" }
                Integer c2 = after.getInt32("c2");
                long expectedMillis = Duration.ofHours(17).plusMinutes(51).plusSeconds(4).plusMillis(780).toMillis();
                assertThat(c2).isEqualTo((int)expectedMillis);
            }
        });
    }

    private void assertTimestamp(String c4) {
        // '2014-09-08 17:51:04.777'
        // MySQL container is in UTC and the test time is during summer time period
        ZonedDateTime expectedTimestamp = ZonedDateTime.ofInstant(
                LocalDateTime.parse("2014-09-08T17:51:04.780").atZone(ZoneId.of("UTC")).toInstant(),
                ZoneId.systemDefault());
        ZoneId defaultZoneId = ZoneId.systemDefault();
        ZonedDateTime c4DateTime = ZonedDateTime.parse(c4, ZonedTimestamp.FORMATTER).withZoneSameInstant(defaultZoneId);
        assertThat(c4DateTime.getYear()).isEqualTo(expectedTimestamp.getYear());
        assertThat(c4DateTime.getMonth()).isEqualTo(expectedTimestamp.getMonth());
        assertThat(c4DateTime.getDayOfMonth()).isEqualTo(expectedTimestamp.getDayOfMonth());
        assertThat(c4DateTime.getHour()).isEqualTo(expectedTimestamp.getHour());
        assertThat(c4DateTime.getMinute()).isEqualTo(expectedTimestamp.getMinute());
        assertThat(c4DateTime.getSecond()).isEqualTo(expectedTimestamp.getSecond());
        assertThat(c4DateTime.getNano()).isEqualTo(expectedTimestamp.getNano());
        // We're running the connector in the same timezone as the server, so the timezone in the timestamp
        // should match our current offset ...
        LocalDateTime expectedLocalDateTime = LocalDateTime.parse("2014-09-08T17:51:04.780");
        ZoneOffset expectedOffset = defaultZoneId.getRules().getOffset(expectedLocalDateTime);
        assertThat(c4DateTime.getOffset()).isEqualTo(expectedOffset);
    }

    private Duration toDuration(String duration) {
        return Duration.parse(duration);
    }
}

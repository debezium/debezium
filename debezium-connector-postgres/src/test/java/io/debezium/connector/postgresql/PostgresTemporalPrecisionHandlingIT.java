/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.AbstractRecordsProducerTest.INSERT_ARRAY_TYPES_STMT;
import static io.debezium.connector.postgresql.AbstractRecordsProducerTest.INSERT_DATE_TIME_TYPES_STMT;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTemporal;
import io.debezium.time.StructuredTime;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTime;
import io.debezium.time.StructuredZonedTimestamp;
import io.debezium.util.Testing;

/**
 * Integration test to test postgres with ISOSTRING {@link TemporalPrecisionMode}.
 *
 * @author Ismail Simsek
 */
public class PostgresTemporalPrecisionHandlingIT extends AbstractAsyncEngineConnectorTest {

    static String TOPIC_NAME = topicName("temporaltype.test_data_types");

    @BeforeAll
    static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    void before() {
        dropDefaultPublication();
        createTable();
        initializeConnectorTestFramework();
    }

    @AfterEach
    void after() throws SQLException {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        dropDefaultPublication();
    }

    private void dropDefaultPublication() {
        TestHelper.execute("DROP PUBLICATION IF EXISTS " + ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME);
    }

    public void createTable() {
        TestHelper.execute("DROP SCHEMA IF EXISTS temporaltype CASCADE;");
        TestHelper.execute("CREATE SCHEMA IF NOT EXISTS temporaltype ;");
        TestHelper.execute("""
                              CREATE TABLE IF NOT EXISTS temporaltype.test_data_types
                              (
                                  c_id INTEGER             ,
                                  c_json JSON              ,
                                  c_jsonb JSONB            ,
                                  c_date DATE              ,
                                  c_timestamp0 TIMESTAMP(0),
                                  c_timestamp1 TIMESTAMP(1),
                                  c_timestamp2 TIMESTAMP(2),
                                  c_timestamp3 TIMESTAMP(3),
                                  c_timestamp4 TIMESTAMP(4),
                                  c_timestamp5 TIMESTAMP(5),
                                  c_timestamp6 TIMESTAMP(6),
                                  c_timestamptz TIMESTAMPTZ,
                                  c_time TIME WITH TIME ZONE,
                                  c_time_whtz TIME WITHOUT TIME ZONE,
                                  c_interval INTERVAL,
                                  PRIMARY KEY(c_id)
                              ) ;
                          ALTER TABLE temporaltype.test_data_types REPLICA IDENTITY FULL;
                """);
    }

    public Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    public Struct getBefore(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    @Test
    void shouldConvertTemporalsToIsoString() throws Exception {
        Testing.Print.disable();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                // .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ISOSTRING)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        // wait for snapshot completion
        TestHelper.execute("""
                INSERT INTO temporaltype.test_data_types
                VALUES (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL );""");

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 1);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 1);
        assertEquals(after.get("c_date"), null);
        assertEquals(after.get("c_timestamp0"), null);
        assertEquals(after.get("c_timestamp1"), null);
        assertEquals(after.get("c_timestamp2"), null);
        assertEquals(after.get("c_timestamp3"), null);
        assertEquals(after.get("c_timestamp4"), null);
        assertEquals(after.get("c_timestamp5"), null);
        assertEquals(after.get("c_timestamp6"), null);
        assertEquals(after.get("c_timestamptz"), null);
        assertEquals(after.get("c_time"), null);
        assertEquals(after.get("c_time_whtz"), null);
        assertEquals(after.get("c_interval"), null);

        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 2);
        after = getAfter(insertRecord);

        assertEquals(after.get("c_id"), 2);
        // '2017-09-15'::DATE
        assertEquals(after.get("c_date"), "2017-09-15Z");
        // '2019-07-09 02:28:57+01' ,
        assertEquals(after.get("c_timestamp0"), "2019-07-09T02:28:57Z");
        // '2019-07-09 02:28:57.1+01'
        assertEquals(after.get("c_timestamp1"), "2019-07-09T02:28:57.1Z");
        // '2019-07-09 02:28:57.12+01' ,
        assertEquals(after.get("c_timestamp2"), "2019-07-09T02:28:57.12Z");
        assertEquals(after.get("c_timestamp3"), "2019-07-09T02:28:57.123Z");
        assertEquals(after.get("c_timestamp4"), "2019-07-09T02:28:57.1234Z");
        assertEquals(after.get("c_timestamp5"), "2019-07-09T02:28:57.12345Z");
        assertEquals(after.get("c_timestamp6"), "2019-07-09T02:28:57.123456Z");
        // '2019-07-09 02:28:10.123456+01' > TEST Hour changes to UTC!
        assertEquals(after.get("c_timestamptz"), "2019-07-09T01:28:10.123456Z");
        // '04:05:11 PST' (-8) Test Hour changes to UTC
        assertEquals(after.get("c_time"), "12:05:11Z");
        // '04:05:11.789'
        assertEquals(after.get("c_time_whtz"), "04:05:11.789Z");
        // INTERVAL '1' YEAR
        assertEquals(after.get("c_interval"), 31557600000000L);
        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22', '04:05:22.789', INTERVAL '10' DAY )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 3);
        after = getAfter(insertRecord);
        // validate right record received
        assertEquals(after.get("c_id"), 3);
        stopConnector();
    }

    @Test
    void shouldConvertTemporalsMicroseconds() throws Exception {
        Testing.Print.disable();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                // .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.MICROSECONDS)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        // wait for snapshot completion
        TestHelper.execute("""
                INSERT INTO temporaltype.test_data_types
                VALUES (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL );""");

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 1);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 1);
        assertEquals(after.get("c_date"), null);
        assertEquals(after.get("c_timestamp0"), null);
        assertEquals(after.get("c_timestamp1"), null);
        assertEquals(after.get("c_timestamp2"), null);
        assertEquals(after.get("c_timestamp3"), null);
        assertEquals(after.get("c_timestamp4"), null);
        assertEquals(after.get("c_timestamp5"), null);
        assertEquals(after.get("c_timestamp6"), null);
        assertEquals(after.get("c_timestamptz"), null);
        assertEquals(after.get("c_time"), null);
        assertEquals(after.get("c_time_whtz"), null);
        assertEquals(after.get("c_interval"), null);

        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 2);
        after = getAfter(insertRecord);

        assertEquals(after.get("c_id"), 2);
        // '2017-09-15'::DATE
        assertEquals(after.get("c_date"), 17424);
        // '2019-07-09 02:28:57+01' ,
        assertEquals(after.get("c_timestamp0"), 1562639337000000L);
        // '2019-07-09 02:28:57.1+01'
        assertEquals(after.get("c_timestamp1"), 1562639337100000L);
        // '2019-07-09 02:28:57.12+01' ,
        assertEquals(after.get("c_timestamp2"), 1562639337120000L);
        assertEquals(after.get("c_timestamp3"), 1562639337123000L);
        assertEquals(after.get("c_timestamp4"), 1562639337123400L);
        assertEquals(after.get("c_timestamp5"), 1562639337123450L);
        assertEquals(after.get("c_timestamp6"), 1562639337123456L);
        // '2019-07-09 02:28:10.123456+01' > TEST Hour changes to UTC!
        assertEquals(after.get("c_timestamptz"), "2019-07-09T01:28:10.123456Z");
        // '04:05:11 PST' (-8) Test Hour changes to UTC
        assertEquals(after.get("c_time"), "12:05:11Z");
        // '04:05:11.789'
        assertEquals(after.get("c_time_whtz"), 14711789000L);
        // INTERVAL '1' YEAR
        assertEquals(after.get("c_interval"), 31557600000000L);
        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22', '04:05:22.789', INTERVAL '10' DAY )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 3);
        after = getAfter(insertRecord);
        // validate right record received
        assertEquals(after.get("c_id"), 3);
        stopConnector();
    }

    @Test
    void shouldConvertTemporalsNanoseconds() throws Exception {
        Testing.Print.disable();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.NANOSECONDS)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        // wait for snapshot completion
        TestHelper.execute("""
                INSERT INTO temporaltype.test_data_types
                VALUES (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL );""");

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 1);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 1);
        assertEquals(after.get("c_date"), null);
        assertEquals(after.get("c_timestamp0"), null);
        assertEquals(after.get("c_timestamp1"), null);
        assertEquals(after.get("c_timestamp2"), null);
        assertEquals(after.get("c_timestamp3"), null);
        assertEquals(after.get("c_timestamp4"), null);
        assertEquals(after.get("c_timestamp5"), null);
        assertEquals(after.get("c_timestamp6"), null);
        assertEquals(after.get("c_timestamptz"), null);
        assertEquals(after.get("c_time"), null);
        assertEquals(after.get("c_time_whtz"), null);
        assertEquals(after.get("c_interval"), null);

        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 2);
        after = getAfter(insertRecord);

        assertEquals(after.get("c_id"), 2);
        // '2017-09-15'::DATE
        assertEquals(after.get("c_date"), 17424);
        // '2019-07-09 02:28:57+01' ,
        assertEquals(after.get("c_timestamp0"), 1562639337000000000L);
        // '2019-07-09 02:28:57.1+01'
        assertEquals(after.get("c_timestamp1"), 1562639337100000000L);
        // '2019-07-09 02:28:57.12+01' ,
        assertEquals(after.get("c_timestamp2"), 1562639337120000000L);
        assertEquals(after.get("c_timestamp3"), 1562639337123000000L);
        assertEquals(after.get("c_timestamp4"), 1562639337123400000L);
        assertEquals(after.get("c_timestamp5"), 1562639337123450000L);
        assertEquals(after.get("c_timestamp6"), 1562639337123456000L);
        // '2019-07-09 02:28:10.123456+01' > TEST Hour changes to UTC!
        assertEquals(after.get("c_timestamptz"), "2019-07-09T01:28:10.123456Z");
        // '04:05:11 PST' (-8) Test Hour changes to UTC
        assertEquals(after.get("c_time"), "12:05:11Z");
        // '04:05:11.789'
        assertEquals(after.get("c_time_whtz"), 14711789000000L);
        // INTERVAL '1' YEAR
        assertEquals(after.get("c_interval"), 31557600000000L);
        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22', '04:05:22.789', INTERVAL '10' DAY )
                        ;""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 3);
        after = getAfter(insertRecord);

        assertEquals(after.get("c_id"), 3);
        stopConnector();
    }

    @Test
    void shouldConvertInfinityTimestampsToLongMinMax() throws Exception {
        Testing.Print.disable();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.NANOSECONDS)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        TestHelper.execute("""
                INSERT INTO temporaltype.test_data_types
                VALUES (10 , NULL, NULL, NULL, 'infinity', 'infinity', 'infinity', 'infinity', 'infinity', 'infinity', 'infinity', NULL, NULL, NULL, NULL );""");

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 10);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 10);
        assertEquals(after.get("c_timestamp0"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp1"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp2"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp3"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp4"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp5"), Long.MAX_VALUE);
        assertEquals(after.get("c_timestamp6"), Long.MAX_VALUE);

        TestHelper.execute("""
                INSERT INTO temporaltype.test_data_types
                VALUES (11 , NULL, NULL, NULL, '-infinity', '-infinity', '-infinity', '-infinity', '-infinity', '-infinity', '-infinity', NULL, NULL, NULL, NULL );""");

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 11);
        after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 11);
        assertEquals(after.get("c_timestamp0"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp1"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp2"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp3"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp4"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp5"), Long.MIN_VALUE);
        assertEquals(after.get("c_timestamp6"), Long.MIN_VALUE);

        stopConnector();
    }

    @Test
    void shouldConvertTemporalsToStructuredValues() throws Exception {
        Testing.Print.disable();
        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                // pgoutput preserves the PostgreSQL text representation needed to verify far-future values.
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.STRUCTURED)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (
                            20,
                            NULL,
                            NULL,
                            'infinity',
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            '294276-12-31 23:59:59.999999',
                            'infinity',
                            '04:05:11 PST',
                            '04:05:11.789',
                            INTERVAL '1 year 2 months 3 days 04:05:06.789'
                        );
                        """);

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 20);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 20);

        assertEquals(StructuredDate.SCHEMA_NAME, after.schema().field("c_date").schema().name());
        assertStructuredSpecialValue(after.getStruct("c_date"), StructuredTemporal.POSITIVE_INFINITY);

        assertEquals(StructuredTimestamp.SCHEMA_NAME, after.schema().field("c_timestamp6").schema().name());
        Struct timestamp = after.getStruct("c_timestamp6");
        assertNull(timestamp.getString(StructuredTemporal.SPECIAL_VALUE_FIELD));
        assertEquals(294276, timestamp.getInt32(StructuredTemporal.YEAR_FIELD));
        assertEquals((byte) 12, timestamp.getInt8(StructuredTemporal.MONTH_FIELD));
        assertEquals((byte) 31, timestamp.getInt8(StructuredTemporal.DAY_FIELD));
        assertEquals((byte) 23, timestamp.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 59, timestamp.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 59, timestamp.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(999_999_000, timestamp.getInt32(StructuredTemporal.NANOS_FIELD));

        assertEquals(StructuredZonedTimestamp.SCHEMA_NAME, after.schema().field("c_timestamptz").schema().name());
        assertStructuredSpecialValue(after.getStruct("c_timestamptz"), StructuredTemporal.POSITIVE_INFINITY);

        assertEquals(StructuredZonedTime.SCHEMA_NAME, after.schema().field("c_time").schema().name());
        Struct zonedTime = after.getStruct("c_time");
        // '04:05:11 PST' (-08): STRUCTURED mode preserves the raw clock and the original offset (no UTC shift).
        assertEquals((byte) 4, zonedTime.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 5, zonedTime.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 11, zonedTime.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(-8 * 3600, zonedTime.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD));

        assertEquals(StructuredTime.SCHEMA_NAME, after.schema().field("c_time_whtz").schema().name());
        Struct time = after.getStruct("c_time_whtz");
        assertEquals((byte) 4, time.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 5, time.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 11, time.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(789_000_000, time.getInt32(StructuredTemporal.NANOS_FIELD));

        assertEquals(StructuredDuration.SCHEMA_NAME, after.schema().field("c_interval").schema().name());
        Struct interval = after.getStruct("c_interval");
        assertEquals(1, interval.getInt32(StructuredTemporal.YEARS_FIELD));
        assertEquals(2, interval.getInt32(StructuredTemporal.MONTHS_FIELD));
        assertEquals(3, interval.getInt32(StructuredTemporal.DAYS_FIELD));
        assertEquals(4, interval.getInt32(StructuredTemporal.HOURS_FIELD));
        assertEquals(5, interval.getInt32(StructuredTemporal.MINUTES_FIELD));
        assertEquals(6L, interval.getInt64(StructuredTemporal.SECONDS_FIELD));
        assertEquals(789_000_000, interval.getInt32(StructuredTemporal.NANOS_FIELD));

        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (
                            21,
                            NULL,
                            NULL,
                            '-infinity',
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            '-infinity',
                            '-infinity',
                            NULL,
                            NULL,
                            NULL
                        );
                        """);

        records = consumeRecordsByTopic(1);
        insertRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, insertRecord.topic());
        VerifyRecord.isValidInsert(insertRecord, "c_id", 21);
        after = getAfter(insertRecord);
        assertEquals(after.get("c_id"), 21);
        assertStructuredSpecialValue(after.getStruct("c_date"), StructuredTemporal.NEGATIVE_INFINITY);
        assertStructuredSpecialValue(after.getStruct("c_timestamp6"), StructuredTemporal.NEGATIVE_INFINITY);
        assertStructuredSpecialValue(after.getStruct("c_timestamptz"), StructuredTemporal.NEGATIVE_INFINITY);

        stopConnector();
    }

    @Test
    void shouldConvertSnapshotTemporalsToStructuredValues() throws Exception {
        Testing.Print.disable();
        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types
                        VALUES (
                            30,
                            NULL,
                            NULL,
                            'infinity',
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            '294276-12-31 23:59:59.999999',
                            'infinity',
                            '04:05:11 PST',
                            '04:05:11.789',
                            INTERVAL '1 year 2 months 3 days 04:05:06.789'
                        );
                        """);

        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.STRUCTURED)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(1);
        final SourceRecord readRecord = records.recordsForTopic(TOPIC_NAME).get(0);
        assertEquals(TOPIC_NAME, readRecord.topic());
        VerifyRecord.isValidRead(readRecord, "c_id", 30);
        final Struct after = getAfter(readRecord);
        assertEquals(after.get("c_id"), 30);

        assertEquals(StructuredDate.SCHEMA_NAME, after.schema().field("c_date").schema().name());
        assertStructuredSpecialValue(after.getStruct("c_date"), StructuredTemporal.POSITIVE_INFINITY);

        assertEquals(StructuredTimestamp.SCHEMA_NAME, after.schema().field("c_timestamp6").schema().name());
        final Struct timestamp = after.getStruct("c_timestamp6");
        assertNull(timestamp.getString(StructuredTemporal.SPECIAL_VALUE_FIELD));
        assertEquals(294276, timestamp.getInt32(StructuredTemporal.YEAR_FIELD));
        assertEquals((byte) 12, timestamp.getInt8(StructuredTemporal.MONTH_FIELD));
        assertEquals((byte) 31, timestamp.getInt8(StructuredTemporal.DAY_FIELD));
        assertEquals((byte) 23, timestamp.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 59, timestamp.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 59, timestamp.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(999_999_000, timestamp.getInt32(StructuredTemporal.NANOS_FIELD));

        assertEquals(StructuredZonedTimestamp.SCHEMA_NAME, after.schema().field("c_timestamptz").schema().name());
        assertStructuredSpecialValue(after.getStruct("c_timestamptz"), StructuredTemporal.POSITIVE_INFINITY);

        assertEquals(StructuredZonedTime.SCHEMA_NAME, after.schema().field("c_time").schema().name());
        final Struct zonedTime = after.getStruct("c_time");
        // '04:05:11 PST' (-08): STRUCTURED mode preserves the raw clock and the original offset (no UTC shift).
        assertEquals((byte) 4, zonedTime.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 5, zonedTime.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 11, zonedTime.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(-8 * 3600, zonedTime.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD));

        assertEquals(StructuredTime.SCHEMA_NAME, after.schema().field("c_time_whtz").schema().name());
        final Struct time = after.getStruct("c_time_whtz");
        assertEquals((byte) 4, time.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 5, time.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 11, time.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(789_000_000, time.getInt32(StructuredTemporal.NANOS_FIELD));

        assertEquals(StructuredDuration.SCHEMA_NAME, after.schema().field("c_interval").schema().name());
        final Struct interval = after.getStruct("c_interval");
        assertEquals(1, interval.getInt32(StructuredTemporal.YEARS_FIELD));
        assertEquals(2, interval.getInt32(StructuredTemporal.MONTHS_FIELD));
        assertEquals(3, interval.getInt32(StructuredTemporal.DAYS_FIELD));
        assertEquals(4, interval.getInt32(StructuredTemporal.HOURS_FIELD));
        assertEquals(5, interval.getInt32(StructuredTemporal.MINUTES_FIELD));
        assertEquals(6L, interval.getInt64(StructuredTemporal.SECONDS_FIELD));
        assertEquals(789_000_000, interval.getInt32(StructuredTemporal.NANOS_FIELD));

        stopConnector();
    }

    @Test
    void shouldPreserveTimetzOffsetAndBoundaryInStructuredMode() throws Exception {
        Testing.Print.disable();
        // PostgreSQL TIMETZ records the offset as stored (no session-TZ adjustment) and allows the end-of-day
        // boundary 24:00:00. STRUCTURED mode must preserve both, which OffsetTime/LocalTime cannot represent.
        TestHelper.execute(
                """
                        INSERT INTO temporaltype.test_data_types (c_id, c_time)
                        VALUES (31, '24:00:00+05:30'), (32, '13:51:30.123789+02:00');
                        """);

        final PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "temporaltype")
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.STRUCTURED)
                .build());
        start(PostgresConnector.class, config.getConfig());
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> read = records.recordsForTopic(TOPIC_NAME);
        assertEquals(2, read.size());

        // 24:00:00+05:30 -> raw clock preserved (hour 24) with the original +05:30 offset.
        final Struct boundary = getAfter(read.get(0));
        assertEquals(31, boundary.get("c_id"));
        assertEquals(StructuredZonedTime.SCHEMA_NAME, boundary.schema().field("c_time").schema().name());
        final Struct boundaryTime = boundary.getStruct("c_time");
        assertEquals((byte) 24, boundaryTime.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 0, boundaryTime.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 0, boundaryTime.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(0, boundaryTime.getInt32(StructuredTemporal.NANOS_FIELD));
        assertEquals(5 * 3600 + 30 * 60, boundaryTime.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD));

        // 13:51:30.123789+02:00 -> sub-second precision and the positive offset are both preserved.
        final Struct fractional = getAfter(read.get(1));
        assertEquals(32, fractional.get("c_id"));
        final Struct fractionalTime = fractional.getStruct("c_time");
        assertEquals((byte) 13, fractionalTime.getInt8(StructuredTemporal.HOUR_FIELD));
        assertEquals((byte) 51, fractionalTime.getInt8(StructuredTemporal.MINUTE_FIELD));
        assertEquals((byte) 30, fractionalTime.getInt8(StructuredTemporal.SECOND_FIELD));
        assertEquals(123_789_000, fractionalTime.getInt32(StructuredTemporal.NANOS_FIELD));
        assertEquals(2 * 3600, fractionalTime.getInt32(StructuredTemporal.OFFSET_SECONDS_FIELD));

        stopConnector();
    }

    @Test
    void shouldReceiveDeletesWithInfinityDate() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute("ALTER TABLE time_table REPLICA IDENTITY FULL");
        // insert data and time data
        TestHelper.execute(INSERT_DATE_TIME_TYPES_STMT);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ISOSTRING)
                .build());
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic("test_server.public.time_table").get(0);
        VerifyRecord.isValidRead(insertRecord, "pk", 1);
        Struct after = getAfter(insertRecord);
        // somehow on github pipeline it gets +292278994-08-16Z vs +292278994-08-17Z
        assertTrue(after.get("date_pinf").toString().contains("+292278994-08-"));
        // somehow on github pipeline it fails expected:<+292269055-12-02Z> but was:<+292269055-12-03Z>
        assertTrue(after.get("date_ninf").toString().contains("+292269055-12-"));
        assertEquals(after.get("tz_max"), "+294247-01-01T23:59:59.999999Z");
        assertEquals(after.get("tz_min"), "-4713-12-31T23:59:59.999999Z");
        assertEquals(after.get("ts_pinf"), "+294247-01-10T04:00:25.2Z");
        assertEquals(after.get("ts_ninf"), "-290308-12-21T19:59:27.6Z");
        assertEquals(after.get("tz_pinf"), "infinity");
        assertEquals(after.get("tz_ninf"), "-infinity");
    }

    @Test
    void shouldReceiveChangesForInsertsWithArrayTypes() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.execute("ALTER TABLE time_table REPLICA IDENTITY FULL");
        // insert data and time data
        TestHelper.execute(INSERT_ARRAY_TYPES_STMT);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ISOSTRING)
                .build());
        assertConnectorIsRunning();

        SourceRecords records = consumeRecordsByTopic(1);
        SourceRecord insertRecord = records.recordsForTopic("test_server.public.array_table").get(0);
        VerifyRecord.isValidRead(insertRecord, "pk", 1);
        Struct after = getAfter(insertRecord);
        assertEquals(after.get("tsrange_array").toString(), "[[\"2019-03-31 15:30:00\",infinity), [\"2019-03-31 15:30:00\",\"2019-04-30 15:30:00\"]]");
        assertEquals(after.get("daterange_array").toString(), "[[2019-03-31,infinity), [2019-03-31,2019-04-30)]");
    }

    private void assertStructuredSpecialValue(Struct value, String expectedSpecialValue) {
        assertEquals(expectedSpecialValue, value.getString(StructuredTemporal.SPECIAL_VALUE_FIELD));
        assertNull(value.get(StructuredTemporal.YEAR_FIELD));
        assertNull(value.get(StructuredTemporal.MONTH_FIELD));
        assertNull(value.get(StructuredTemporal.DAY_FIELD));
    }

}

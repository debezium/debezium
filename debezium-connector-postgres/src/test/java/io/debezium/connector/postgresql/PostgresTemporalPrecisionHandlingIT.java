/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.AbstractRecordsProducerTest.INSERT_ARRAY_TYPES_STMT;
import static io.debezium.connector.postgresql.AbstractRecordsProducerTest.INSERT_DATE_TIME_TYPES_STMT;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;

/**
 * Integration test to test postgres with ISOSTRING {@link TemporalPrecisionMode}.
 *
 * @author Ismail Simsek
 */
public class PostgresTemporalPrecisionHandlingIT extends AbstractAsyncEngineConnectorTest {

    static String TOPIC_NAME = topicName("temporaltype.test_data_types");

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
        createTable();
    }

    @After
    public void after() throws SQLException {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
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
    public void shouldConvertTemporalsToIsoString() throws Exception {
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
    public void shouldConvertTemporalsMicroseconds() throws Exception {
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
    public void shouldConvertTemporalsNanoseconds() throws Exception {
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
    public void shouldReceiveDeletesWithInfinityDate() throws Exception {
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
        assertEquals(after.get("tz_min"), "-4713-11-23T23:59:59.999999Z");
        assertEquals(after.get("ts_pinf"), "+294247-01-10T04:00:25.2Z");
        assertEquals(after.get("ts_ninf"), "-290308-12-21T19:59:27.6Z");
        assertEquals(after.get("tz_pinf"), "infinity");
        assertEquals(after.get("tz_ninf"), "-infinity");
    }

    @Test
    public void shouldReceiveChangesForInsertsWithArrayTypes() throws Exception {
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

}

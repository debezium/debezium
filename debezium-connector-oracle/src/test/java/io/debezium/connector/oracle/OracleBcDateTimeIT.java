/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests the behavior of temporal data types that store BC era or pre-Gregorian cut-over values.
 *
 * @author Chris Cranford
 */
public class OracleBcDateTimeIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.bc_datetime_test");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute("CREATE TABLE bc_datetime_test ("
                + "id numeric(9,0) primary key, "
                + "val_date date, "
                + "val_ts timestamp(6), "
                + "val_tstz timestamp(6) with time zone, "
                + "val_tsltz timestamp(6) with local time zone)");
        TestHelper.streamTable(connection, "debezium.bc_datetime_test");
    }

    @AfterEach
    void after() throws Exception {
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, "debezium.bc_datetime_test");
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#1286")
    public void shouldSnapshotBcAndPreGregorianCutoverValues() throws Exception {
        // 2018 BC (Oracle year -2018, ISO proleptic year -2017)
        connection.executeWithoutCommitting("INSERT INTO debezium.bc_datetime_test VALUES ("
                + "1"
                + ", TO_DATE('-2018-03-27 12:34:56', 'SYYYY-MM-DD HH24:MI:SS')"
                + ", TO_TIMESTAMP('-2018-03-27 12:34:56.00789', 'SYYYY-MM-DD HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP_TZ('-2018-03-27 01:34:56.00789 -11:00', 'SYYYY-MM-DD HH24:MI:SS.FF5 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('-2018-03-27 01:34:56.00789 -11:00', 'SYYYY-MM-DD HH24:MI:SS.FF5 TZH:TZM')"
                + ")");
        // 1 BC (Oracle year -1, ISO proleptic year 0)
        connection.executeWithoutCommitting("INSERT INTO debezium.bc_datetime_test VALUES ("
                + "2"
                + ", TO_DATE('-0001-12-31 23:59:59', 'SYYYY-MM-DD HH24:MI:SS')"
                + ", TO_TIMESTAMP('-0001-12-31 23:59:59.99999', 'SYYYY-MM-DD HH24:MI:SS.FF5')"
                + ", NULL"
                + ", NULL"
                + ")");
        // 1500 AD, before the Gregorian calendar cut-over in 1582
        connection.executeWithoutCommitting("INSERT INTO debezium.bc_datetime_test VALUES ("
                + "3"
                + ", TO_DATE('1500-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
                + ", TO_TIMESTAMP('1500-03-01 12:34:56.00789', 'YYYY-MM-DD HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP_TZ('1500-03-01 01:34:56.00789 -05:00', 'YYYY-MM-DD HH24:MI:SS.FF5 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('1500-03-01 01:34:56.00789 -05:00', 'YYYY-MM-DD HH24:MI:SS.FF5 TZH:TZM')"
                + ")");
        connection.execute("COMMIT");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.BC_DATETIME_TEST")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> testRecords = records.recordsForTopic("server1.DEBEZIUM.BC_DATETIME_TEST");
        assertThat(testRecords).hasSize(3);

        Struct after = afterStruct(testRecords.get(0));
        assertThat(after.get("VAL_DATE")).isEqualTo(toEpochMillis(LocalDateTime.of(-2017, 3, 27, 12, 34, 56)));
        assertThat(after.get("VAL_TS")).isEqualTo(toEpochMicros(LocalDateTime.of(-2017, 3, 27, 12, 34, 56, 7_890_000)));
        assertThat(after.get("VAL_TSTZ")).isEqualTo("-2017-03-27T01:34:56.007890-11:00");
        assertThat(after.get("VAL_TSLTZ")).isEqualTo("-2017-03-27T12:34:56.007890Z");

        after = afterStruct(testRecords.get(1));
        assertThat(after.get("VAL_DATE")).isEqualTo(toEpochMillis(LocalDateTime.of(0, 12, 31, 23, 59, 59)));
        assertThat(after.get("VAL_TS")).isEqualTo(toEpochMicros(LocalDateTime.of(0, 12, 31, 23, 59, 59, 999_990_000)));
        assertThat(after.get("VAL_TSTZ")).isNull();
        assertThat(after.get("VAL_TSLTZ")).isNull();

        after = afterStruct(testRecords.get(2));
        assertThat(after.get("VAL_DATE")).isEqualTo(toEpochMillis(LocalDateTime.of(1500, 3, 1, 0, 0)));
        assertThat(after.get("VAL_TS")).isEqualTo(toEpochMicros(LocalDateTime.of(1500, 3, 1, 12, 34, 56, 7_890_000)));
        assertThat(after.get("VAL_TSTZ")).isEqualTo("1500-03-01T01:34:56.007890-05:00");
        assertThat(after.get("VAL_TSLTZ")).isEqualTo("1500-03-01T06:34:56.007890Z");
    }

    private static Struct afterStruct(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(FieldName.AFTER);
    }

    private static long toEpochMillis(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static long toEpochMicros(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000 + dateTime.getNano() / 1_000;
    }
}
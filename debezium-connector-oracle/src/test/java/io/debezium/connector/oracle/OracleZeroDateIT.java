/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.OracleZeroDates;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.FieldName;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

/**
 * Tests the behavior of zero dates ({@code 0000-00-00 00:00:00}), which can be stored by OCI clients but
 * cannot be written using SQL date conversion functions.
 *
 * @author Chris Cranford
 */
public class OracleZeroDateIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "debezium.zero_date_test";
    private static final String TOPIC = "server1.DEBEZIUM.ZERO_DATE_TEST";

    private OracleConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, TABLE);

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute("CREATE TABLE zero_date_test ("
                + "id numeric(9,0) primary key, "
                + "val_nullable date, "
                + "val_default date default TO_DATE('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') not null, "
                + "val_required date not null, "
                + "val_ts_nullable timestamp(6), "
                + "val_ts_default timestamp(6) default TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') not null, "
                + "val_ts_required timestamp(6) not null, "
                + "val_ts9_required timestamp(9) not null, "
                + "val_tstz_nullable timestamp(6) with time zone, "
                + "val_tstz_required timestamp(6) with time zone not null, "
                + "val_tsltz_nullable timestamp(6) with local time zone, "
                + "val_tsltz_required timestamp(6) with local time zone not null)");
        TestHelper.streamTable(connection, TABLE);
    }

    @AfterEach
    void after() throws Exception {
        if (connection != null && connection.isConnected()) {
            TestHelper.dropTable(connection, TABLE);
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldSnapshotZeroDates() throws Exception {
        insertRows();

        start(OracleConnector.class, defaultConfig().build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        assertRowsWithoutConverter();
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldStreamZeroDates() throws Exception {
        start(OracleConnector.class, defaultConfig().build());
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        insertRows();

        assertRowsWithoutConverter();
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldSnapshotZeroDatesWithFallbackConverter() throws Exception {
        insertRows();

        start(OracleConnector.class, converterConfig().build());
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        assertRowsWithConverter();
    }

    @Test
    @FixFor("debezium/dbz#3055")
    public void shouldStreamZeroDatesWithFallbackConverter() throws Exception {
        start(OracleConnector.class, converterConfig().build());
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        insertRows();

        assertRowsWithConverter();
    }

    private static Configuration.Builder defaultConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.ZERO_DATE_TEST")
                .with(OracleConnectorConfig.EVENT_CONVERTING_FAILURE_HANDLING_MODE, "fail");
    }

    private static Configuration.Builder converterConfig() {
        final String table = TestHelper.getDatabaseName() + ".DEBEZIUM.ZERO_DATE_TEST";
        return defaultConfig()
                .with(OracleConnectorConfig.CUSTOM_CONVERTERS, "zerodate")
                .with("zerodate.type", "io.debezium.connector.oracle.converters.ZeroDateFallbackConverter")
                .with("zerodate.selector", ".*\\.ZERO_DATE_TEST\\.VAL_(DEFAULT|REQUIRED|TS_DEFAULT|TS_REQUIRED|TS9_REQUIRED)")
                .with("zerodate.fallback.date", "NULL")
                .with("zerodate.fallback.timestamp", "NULL")
                .with("zerodate.column." + table + ".VAL_DEFAULT.fallback", "1900-01-01 00:00:00")
                .with("zerodate.column." + table + ".VAL_TS_DEFAULT.fallback", "1900-01-01 00:00:00.000001")
                .with("zerodate.column." + table + ".VAL_TS9_REQUIRED.fallback", "1900-01-01 00:00:00.000000001");
    }

    private void insertRows() throws Exception {
        // A zero date can only be stored through its internal representation: the DATE is converted from
        // its raw form, and the TIMESTAMP is bound in its raw form, the way OCI clients write it.
        final String sql = "DECLARE d DATE; BEGIN "
                + "DBMS_STATS.CONVERT_RAW_VALUE(HEXTORAW('" + HexFormat.of().formatHex(OracleZeroDates.ZERO_DATE_BYTES) + "'), d); "
                + "INSERT INTO debezium.zero_date_test VALUES (1, d, d, d, ?, ?, ?, ?, ?, ?, ?, ?); END;";
        try (PreparedStatement statement = connection.connection().prepareStatement(sql)) {
            for (int i = 1; i <= 4; i++) {
                statement.setObject(i, new TIMESTAMP(OracleZeroDates.ZERO_TIMESTAMP_BYTES));
            }
            statement.setObject(5, new TIMESTAMPTZ(OracleZeroDates.ZERO_TIMESTAMP_TZ_BYTES));
            statement.setObject(6, new TIMESTAMPTZ(OracleZeroDates.ZERO_TIMESTAMP_TZ_BYTES));
            statement.setObject(7, new TIMESTAMPLTZ(OracleZeroDates.ZERO_TIMESTAMP_BYTES));
            statement.setObject(8, new TIMESTAMPLTZ(OracleZeroDates.ZERO_TIMESTAMP_BYTES));
            statement.execute();
        }
        connection.commit();

        // Legal values with the same lenient representation as the zero date must be unaffected
        connection.execute("INSERT INTO debezium.zero_date_test VALUES (2"
                + ", TO_DATE('-0002-11-30 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
                + ", TO_DATE('0002-11-30 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
                + ", TO_DATE('-0001-01-02 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
                + ", TO_TIMESTAMP('-0002-11-30 12:34:56.123456', 'SYYYY-MM-DD HH24:MI:SS.FF6')"
                + ", TO_TIMESTAMP('0002-11-30 12:34:56.123456', 'SYYYY-MM-DD HH24:MI:SS.FF6')"
                + ", TO_TIMESTAMP('2020-05-17 12:34:56.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
                + ", TO_TIMESTAMP('2020-05-17 12:34:56.123456789', 'YYYY-MM-DD HH24:MI:SS.FF9')"
                + ", TO_TIMESTAMP_TZ('-0002-11-30 01:34:56.123456 -11:00', 'SYYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('2020-05-17 01:34:56.123456 -11:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('-0002-11-30 01:34:56.123456 -11:00', 'SYYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('2020-05-17 01:34:56.123456 -11:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'))");
    }

    private List<SourceRecord> consumeTestRecords() throws Exception {
        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> testRecords = records.recordsForTopic(TOPIC);
        assertThat(testRecords).hasSize(2);
        return testRecords;
    }

    private void assertRowsWithoutConverter() throws Exception {
        final List<SourceRecord> testRecords = consumeTestRecords();

        final LocalDateTime ddlDefault = LocalDateTime.of(2000, 1, 1, 0, 0);
        final Struct after = afterStruct(testRecords.get(0));

        assertThat(after.schema().field("VAL_DEFAULT").schema().defaultValue()).isEqualTo(toEpochMillis(ddlDefault));
        assertThat(after.get("VAL_NULLABLE")).isNull();
        assertThat(after.get("VAL_DEFAULT")).isEqualTo(toEpochMillis(ddlDefault));
        assertThat(after.get("VAL_REQUIRED")).isEqualTo(0L);

        assertThat(after.schema().field("VAL_TS_DEFAULT").schema().defaultValue()).isEqualTo(toEpochMicros(ddlDefault));
        assertThat(after.get("VAL_TS_NULLABLE")).isNull();
        assertThat(after.get("VAL_TS_DEFAULT")).isEqualTo(toEpochMicros(ddlDefault));
        assertThat(after.get("VAL_TS_REQUIRED")).isEqualTo(0L);
        assertThat(after.get("VAL_TS9_REQUIRED")).isEqualTo(0L);

        assertTimeZoneZeroDates(after);
        assertLegalValues(afterStruct(testRecords.get(1)));
    }

    private void assertRowsWithConverter() throws Exception {
        final List<SourceRecord> testRecords = consumeTestRecords();

        final LocalDateTime fallback = LocalDateTime.of(1900, 1, 1, 0, 0);
        final Struct after = afterStruct(testRecords.get(0));

        assertThat(after.schema().field("VAL_REQUIRED").schema().isOptional()).isTrue();
        assertThat(after.schema().field("VAL_DEFAULT").schema().isOptional()).isFalse();
        assertThat(after.get("VAL_NULLABLE")).isNull();
        assertThat(after.get("VAL_DEFAULT")).isEqualTo(toEpochMillis(fallback));
        assertThat(after.get("VAL_REQUIRED")).isNull();

        assertThat(after.schema().field("VAL_TS_REQUIRED").schema().isOptional()).isTrue();
        assertThat(after.schema().field("VAL_TS_DEFAULT").schema().isOptional()).isFalse();
        assertThat(after.schema().field("VAL_TS9_REQUIRED").schema().isOptional()).isFalse();
        assertThat(after.get("VAL_TS_NULLABLE")).isNull();
        assertThat(after.get("VAL_TS_DEFAULT")).isEqualTo(toEpochMicros(fallback) + 1);
        assertThat(after.get("VAL_TS_REQUIRED")).isNull();
        assertThat(after.get("VAL_TS9_REQUIRED")).isEqualTo(toEpochNanos(fallback) + 1);

        // Columns with a time zone are not selected by the converter
        assertTimeZoneZeroDates(after);
        assertLegalValues(afterStruct(testRecords.get(1)));
    }

    private static void assertTimeZoneZeroDates(Struct after) {
        assertThat(after.get("VAL_TSTZ_NULLABLE")).isNull();
        assertThat(after.get("VAL_TSTZ_REQUIRED")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(after.get("VAL_TSLTZ_NULLABLE")).isNull();
        assertThat(after.get("VAL_TSLTZ_REQUIRED")).isEqualTo("1970-01-01T00:00:00Z");
    }

    private static void assertLegalValues(Struct after) {
        // 2 BC is ISO proleptic year -1, 1 BC is ISO proleptic year 0
        assertThat(after.get("VAL_NULLABLE")).isEqualTo(toEpochMillis(LocalDateTime.of(-1, 11, 30, 0, 0)));
        assertThat(after.get("VAL_DEFAULT")).isEqualTo(toEpochMillis(LocalDateTime.of(2, 11, 30, 0, 0)));
        assertThat(after.get("VAL_REQUIRED")).isEqualTo(toEpochMillis(LocalDateTime.of(0, 1, 2, 0, 0)));
        assertThat(after.get("VAL_TS_NULLABLE")).isEqualTo(toEpochMicros(LocalDateTime.of(-1, 11, 30, 12, 34, 56, 123_456_000)));
        assertThat(after.get("VAL_TS_DEFAULT")).isEqualTo(toEpochMicros(LocalDateTime.of(2, 11, 30, 12, 34, 56, 123_456_000)));
        assertThat(after.get("VAL_TS_REQUIRED")).isEqualTo(toEpochMicros(LocalDateTime.of(2020, 5, 17, 12, 34, 56, 123_456_000)));
        assertThat(after.get("VAL_TS9_REQUIRED")).isEqualTo(toEpochNanos(LocalDateTime.of(2020, 5, 17, 12, 34, 56, 123_456_789)));
        assertThat(after.get("VAL_TSTZ_NULLABLE")).isEqualTo("-0001-11-30T01:34:56.123456-11:00");
        assertThat(after.get("VAL_TSTZ_REQUIRED")).isEqualTo("2020-05-17T01:34:56.123456-11:00");
        assertThat(after.get("VAL_TSLTZ_NULLABLE")).isEqualTo("-0001-11-30T12:34:56.123456Z");
        assertThat(after.get("VAL_TSLTZ_REQUIRED")).isEqualTo("2020-05-17T12:34:56.123456Z");
    }

    private static Struct afterStruct(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(FieldName.AFTER);
    }

    private static long toEpochMillis(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static long toEpochMicros(LocalDateTime dateTime) {
        final Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
    }

    private static long toEpochNanos(LocalDateTime dateTime) {
        final Instant instant = dateTime.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.DataQueryMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.time.IsoTimestamp;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.util.Testing;

class SqlServerTimezoneLessTimestampDstGapIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;
    private TimeZone originalTimeZone;

    @BeforeEach
    void before() throws SQLException {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));

        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE datetime2_dst_gap (id int primary key, ts_datetime2 datetime2(7), ts_datetime datetime, ts_smalldatetime smalldatetime)",
                "INSERT INTO datetime2_dst_gap VALUES (1, '2024-03-10 02:30:00.0000000', '2024-03-10 02:30:00.000', '2024-03-10 02:30:00')");
        TestHelper.enableTableCdc(connection, "datetime2_dst_gap");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void after() throws SQLException {
        try {
            stopConnector();
            if (connection != null) {
                connection.close();
            }
        }
        finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @ParameterizedTest(name = "snapshot preserves timezone-less timestamps during DST gap using {0}")
    @MethodSource("temporalPrecisionModes")
    @FixFor("debezium/dbz#2805")
    void snapshotPreservesTimezoneLessTimestampsDuringDstGap(TemporalPrecisionMode precisionMode) throws Exception {
        startConnector(SqlServerConnectorConfig.SnapshotMode.INITIAL, DataQueryMode.FUNCTION, precisionMode);
        SourceRecord record = consumeTableRecord();
        assertTimezoneLessTimestamps(record, precisionMode);
    }

    @ParameterizedTest(name = "streaming preserves timezone-less timestamps during DST gap using {0} and {1}")
    @MethodSource("streamingModes")
    @FixFor("debezium/dbz#2805")
    void streamingPreservesTimezoneLessTimestampsDuringDstGap(DataQueryMode dataQueryMode, TemporalPrecisionMode precisionMode) throws Exception {
        startConnector(SqlServerConnectorConfig.SnapshotMode.NO_DATA, dataQueryMode, precisionMode);
        // Streaming must be connected before the change is written; otherwise a slow CDC poll can miss it.
        TestHelper.waitForStreamingStarted();
        connection.execute(
                "INSERT INTO datetime2_dst_gap VALUES (2, '2024-03-10 02:30:00.0000000', '2024-03-10 02:30:00.000', '2024-03-10 02:30:00')");
        TestHelper.waitForCdcRecord(connection, "datetime2_dst_gap", rs -> rs.getInt("id") == 2);

        SourceRecord record = consumeTableRecord();
        assertTimezoneLessTimestamps(record, precisionMode);
    }

    private static Stream<TemporalPrecisionMode> temporalPrecisionModes() {
        return Stream.of(TemporalPrecisionMode.values());
    }

    private static Stream<Arguments> streamingModes() {
        return Stream.of(DataQueryMode.values())
                .flatMap(dataQueryMode -> temporalPrecisionModes()
                        .map(precisionMode -> Arguments.of(dataQueryMode, precisionMode)));
    }

    private void startConnector(SqlServerConnectorConfig.SnapshotMode snapshotMode, DataQueryMode dataQueryMode,
                                TemporalPrecisionMode precisionMode)
            throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, dataQueryMode)
                .with(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE, precisionMode)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
    }

    private SourceRecord consumeTableRecord() throws InterruptedException {
        String topic = "server1.testDB1.dbo.datetime2_dst_gap";
        List<SourceRecord> records = consumeRecordsByTopicUntil(
                (recordsConsumed, record) -> topic.equals(record.topic()))
                .recordsForTopic(topic);
        assertThat(records)
                .as("connector should emit the timezone-less timestamp change")
                .isNotNull()
                .isNotEmpty();
        return records.get(0);
    }

    private void assertTimezoneLessTimestamps(SourceRecord record, TemporalPrecisionMode precisionMode) {
        Struct after = (Struct) ((Struct) record.value()).get("after");
        // Scales match the declared column types: datetime2(7), datetime, and smalldatetime.
        assertTimestampValue(after, "ts_datetime2", precisionMode, 7);
        assertTimestampValue(after, "ts_datetime", precisionMode, 3);
        assertTimestampValue(after, "ts_smalldatetime", precisionMode, 0);
    }

    private void assertTimestampValue(Struct record, String fieldName, TemporalPrecisionMode precisionMode, int scale) {
        Object actual = record.get(fieldName);
        LocalDateTime expected = LocalDateTime.of(2024, 3, 10, 2, 30);

        switch (precisionMode) {
            case CONNECT:
                assertThat(LocalDateTime.ofInstant(((Date) actual).toInstant(), ZoneOffset.UTC))
                        .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                        .isEqualTo(expected);
                break;
            case ADAPTIVE:
            case ADAPTIVE_TIME_MICROSECONDS:
                assertThat(actual)
                        .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                        .isEqualTo(adaptiveTimestamp(expected, scale));
                break;
            case MICROSECONDS:
                assertThat(actual)
                        .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                        .isEqualTo(MicroTimestamp.toEpochMicros(expected, null));
                break;
            case NANOSECONDS:
                assertThat(actual)
                        .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                        .isEqualTo(NanoTimestamp.toEpochNanos(expected, null));
                break;
            case ISOSTRING:
                assertThat(actual)
                        .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                        .isEqualTo(IsoTimestamp.toIsoString(expected, null));
                break;
            case STRUCTURED:
                Schema structuredSchema = StructuredTimestamp.builder().optional().build();
                new SchemaAndValueField(fieldName, structuredSchema,
                        StructuredTimestamp.from(structuredSchema, expected, scale))
                        .assertFor(record);
                break;
            default:
                throw new IllegalArgumentException("Unexpected precision mode: " + precisionMode);
        }
    }

    private static long adaptiveTimestamp(LocalDateTime value, int scale) {
        if (scale <= 3) {
            return Timestamp.toEpochMillis(value, null);
        }
        if (scale <= 6) {
            return MicroTimestamp.toEpochMicros(value, null);
        }
        return NanoTimestamp.toEpochNanos(value, null);
    }
}

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
import java.util.TimeZone;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.DataQueryMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
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

    @Test
    void snapshotPreservesTimezoneLessTimestampsDuringDstGap() throws Exception {
        startConnector(SqlServerConnectorConfig.SnapshotMode.INITIAL, DataQueryMode.FUNCTION);
        SourceRecord record = consumeTableRecord();
        assertTimezoneLessTimestamps(record);
    }

    @ParameterizedTest(name = "streaming preserves timezone-less timestamps during DST gap using {0} mode")
    @EnumSource(DataQueryMode.class)
    void streamingPreservesTimezoneLessTimestampsDuringDstGap(DataQueryMode dataQueryMode) throws Exception {
        startConnector(SqlServerConnectorConfig.SnapshotMode.NO_DATA, dataQueryMode);
        connection.execute(
                "INSERT INTO datetime2_dst_gap VALUES (2, '2024-03-10 02:30:00.0000000', '2024-03-10 02:30:00.000', '2024-03-10 02:30:00')");
        TestHelper.waitForCdcRecord(connection, "datetime2_dst_gap", rs -> rs.getInt("id") == 2);

        SourceRecord record = consumeTableRecord();
        assertTimezoneLessTimestamps(record);
    }

    private void startConnector(SqlServerConnectorConfig.SnapshotMode snapshotMode, DataQueryMode dataQueryMode) throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, snapshotMode)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, dataQueryMode)
                .with(RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
    }

    private SourceRecord consumeTableRecord() throws InterruptedException {
        return consumeRecordsByTopic(1)
                .recordsForTopic("server1.testDB1.dbo.datetime2_dst_gap")
                .get(0);
    }

    private void assertTimezoneLessTimestamps(SourceRecord record) {
        Struct after = (Struct) ((Struct) record.value()).get("after");
        assertTimestampValue(after, "ts_datetime2");
        assertTimestampValue(after, "ts_datetime");
        assertTimestampValue(after, "ts_smalldatetime");
    }

    private void assertTimestampValue(Struct record, String fieldName) {
        Date actual = (Date) record.get(fieldName);
        LocalDateTime actualUtc = LocalDateTime.ofInstant(actual.toInstant(), ZoneOffset.UTC);

        assertThat(actualUtc)
                .as("%s has no timezone and should preserve the stored wall-clock value", fieldName)
                .isEqualTo(LocalDateTime.of(2024, 3, 10, 2, 30));
    }
}

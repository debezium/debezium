/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogYearIT;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class YearIT extends BinlogYearIT<MariaDbConnector> implements MariaDbCommon {

    private static final String TABLE = "quoted_zero";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-year2-zero.txt")
            .toAbsolutePath();

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("DBZ-2773")
    public void shouldKeepYear2ZeroConsistentAcrossSnapshotAndStreaming(boolean adjuster) throws Exception {
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        final var database = TestHelper.getUniqueDatabase("year2zero", "year_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        try {
            database.createAndInitialize();
            executeStatements(database.getDatabaseName(),
                    "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, y YEAR(2) DEFAULT '0000')",
                    "INSERT INTO " + TABLE + " (id) VALUES (1)",
                    "INSERT INTO " + TABLE + " VALUES (2, '0000'), (3, 0), (4, 2000)");
            final var config = database.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                    .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, database.qualifiedTableName(TABLE))
                    .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                    .with(BinlogConnectorConfig.ENABLE_TIME_ADJUSTER, adjuster)
                    .build();
            start(getConnectorClass(), config);
            assertYears(4, "r", database.topicForTable(TABLE));
            waitForStreamingRunning(getConnectorName(), database.getServerName());
            executeStatements(database.getDatabaseName(),
                    "INSERT INTO " + TABLE + " (id) VALUES (5)",
                    "INSERT INTO " + TABLE + " VALUES (6, '0000'), (7, 0), (8, 2000)");
            assertYears(4, "c", database.topicForTable(TABLE));
        }
        finally {
            stopConnector();
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    private void assertYears(int count, String operation, String topic) throws InterruptedException {
        final var records = consumeRecordsByTopic(count).recordsForTopic(topic);
        assertThat(records).hasSize(count);
        for (SourceRecord record : records) {
            final var envelope = (Struct) record.value();
            assertThat(envelope.getString("op")).isEqualTo(operation);
            final var after = envelope.getStruct("after");
            assertThat(after.getInt32("y")).isEqualTo(expectedYear(after.getInt32("id")));
        }
    }

    private static int expectedYear(int id) {
        return switch (id) {
            case 1, 2, 5, 6 -> 1900;
            case 3, 4, 7, 8 -> 2000;
            default -> throw new IllegalArgumentException("Unexpected id " + id);
        };
    }
}

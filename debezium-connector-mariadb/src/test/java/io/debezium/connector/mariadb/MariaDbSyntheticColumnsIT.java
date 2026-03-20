/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.util.Testing;

/**
 * Integration test for DBZ-1395: MariaDB Connector Fails with Schema Inconsistency on Tables
 * Using Generated Columns in Unique Constraints.
 *
 * <p>When a MariaDB table has a virtual generated column used in a unique index, MariaDB internally
 * creates a synthetic {@code DB_ROW_HASH_*} column to support the constraint. This synthetic column
 * appears in the binlog row events but is not visible via standard JDBC metadata. This test verifies
 * that Debezium correctly handles such tables by:
 * <ol>
 *   <li>Not failing with a schema/row column-count mismatch.</li>
 *   <li>Excluding the synthetic {@code DB_ROW_HASH_*} column from the emitted Kafka record payload.</li>
 * </ol>
 *
 * @author Debezium Authors
 */
public class MariaDbSyntheticColumnsIT extends AbstractBinlogConnectorIT<MariaDbConnector>
        implements MariaDbCommon {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files
            .createTestingPath("file-schema-history-synthetic-cols.txt")
            .toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper
            .getUniqueDatabase("syntheticcols", "mariadb_synthetic_column_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @BeforeEach
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    /**
     * Tests that when streaming changes from a table that has a virtual generated column
     * participating in a unique constraint, Debezium correctly:
     * <ul>
     *   <li>Processes INSERT events without throwing a schema/row length mismatch error.</li>
     *   <li>Emits Kafka records that contain the real application columns (id, text_val, v_col).</li>
     *   <li>Does <em>not</em> include the synthetic internal {@code DB_ROW_HASH_*} column in the payload.</li>
     * </ul>
     */
    @Test
    @FixFor("DBZ-1395")
    public void shouldExcludeSyntheticDbRowHashColumnAndNotFail() throws Exception {
        Configuration config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz1395"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // Stream a change to verify binlog handling does not fail due to DB_ROW_HASH.
        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO dbz1395 (id, text_val) VALUES (1, 'hello')");

        // Consume exactly 1 record (the snapshot had no rows; the DDL only created the table).
        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(DATABASE.topicForTable("dbz1395"));
        assertThat(records)
                .describedAs("Expected exactly 1 CDC record for the insert into dbz1395")
                .hasSize(1);

        SourceRecord record = records.get(0);
        Struct value = (Struct) record.value();
        Struct after = value.getStruct("after");

        // Verify the real application columns are present and correct.
        assertThat(after.schema().field("id")).isNotNull();
        assertThat(after.schema().field("text_val")).isNotNull();
        assertThat(after.schema().field("v_col")).isNotNull();
        assertThat(after.getInt32("id")).isEqualTo(1);
        assertThat(after.getString("text_val")).isEqualTo("hello");
        assertThat(after.getString("v_col")).isEqualTo("HELLO");

        // The synthetic DB_ROW_HASH_* column must NOT appear in the Kafka payload.
        boolean hasSyntheticColumn = after.schema().fields().stream()
                .anyMatch(f -> f.name().startsWith("DB_ROW_HASH"));
        assertThat(hasSyntheticColumn)
                .describedAs("synthetic DB_ROW_HASH column should be excluded from the Kafka payload")
                .isFalse();
    }
}

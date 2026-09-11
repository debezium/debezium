/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Jiri Pechanec
 */
public abstract class BinlogRestartIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-restart.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("restart", "connector_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1276")
    public void shouldNotDuplicateEventsAfterRestart() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("restart_table"))
                .build();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "CREATE TABLE restart_table (id INT PRIMARY KEY, val INT)",
                        "INSERT INTO restart_table VALUES(1, 10)");
            }
        }
        start(getConnectorClass(), config, record -> {
            final Schema schema = record.valueSchema();
            final Struct value = ((Struct) record.value());
            return schema.field("after") != null && value.getStruct("after").getInt32("id").equals(5);
        });

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(15);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("restart_table")).size()).isEqualTo(1);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.connect().setAutoCommit(false);
                connection.execute(
                        "INSERT INTO restart_table VALUES(2,12)",
                        "INSERT INTO restart_table VALUES(3,13)",
                        "INSERT INTO restart_table VALUES(4,14)",
                        "INSERT INTO restart_table VALUES(5,15)",
                        "INSERT INTO restart_table VALUES(6,16)");
            }
        }
        records = consumeRecordsByTopic(3);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("restart_table")).size()).isEqualTo(3);
        assertThat(((Struct) ((SourceRecord) records.recordsForTopic(DATABASE.topicForTable("restart_table")).get(0)).value()).getStruct("after").getInt32("id"))
                .isEqualTo(2);

        waitForEngineShutdown();
        stopConnector();

        start(getConnectorClass(), config);

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("restart_table")).size()).isEqualTo(2);
        assertThat(((Struct) ((SourceRecord) records.recordsForTopic(DATABASE.topicForTable("restart_table")).get(0)).value()).getStruct("after").getInt32("id"))
                .isEqualTo(5);

        stopConnector();
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldReplayDeleteWhenStoppedBeforeTombstoneIsCommitted() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("restart_table"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute(
                    "CREATE TABLE restart_table (id INT PRIMARY KEY, val INT)",
                    "INSERT INTO restart_table VALUES(1, 10)");
        }

        start(getConnectorClass(), config, record -> record.value() == null
                && ((Struct) record.key()).getInt32("id") == 1);

        final SourceRecord snapshotRecord = consumeRecord();
        assertThat(((Struct) snapshotRecord.key()).getInt32("id")).isEqualTo(1);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("DELETE FROM restart_table WHERE id = 1");
        }

        final SourceRecord deleteBeforeStop = consumeRecord();
        assertThat(((Struct) deleteBeforeStop.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) deleteBeforeStop.value()).getString("op")).isEqualTo("d");

        waitForEngineShutdown();
        stopConnector();

        start(getConnectorClass(), config);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("INSERT INTO restart_table VALUES(3, 30)");
        }

        final SourceRecord replayedDelete = consumeRecord();
        assertThat(((Struct) replayedDelete.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) replayedDelete.value()).getString("op")).isEqualTo("d");

        final SourceRecord replayedTombstone = consumeRecord();
        assertThat(((Struct) replayedTombstone.key()).getInt32("id")).isEqualTo(1);
        assertThat(replayedTombstone.value()).isNull();

        final SourceRecord markerCreate = consumeRecord();
        assertThat(((Struct) markerCreate.key()).getInt32("id")).isEqualTo(3);
        assertThat(((Struct) markerCreate.value()).getString("op")).isEqualTo("c");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @FixFor("debezium/dbz#2549")
    public void shouldReplayPrimaryKeyUpdateWhenStoppedBetweenSplitRecords(boolean stopBeforeTombstone) throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("restart_table"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute(
                    "CREATE TABLE restart_table (id INT PRIMARY KEY, val INT)",
                    "INSERT INTO restart_table VALUES(1, 10)");
        }

        start(getConnectorClass(), config, record -> {
            if (stopBeforeTombstone) {
                return record.value() == null && ((Struct) record.key()).getInt32("id") == 1;
            }
            return record.value() != null
                    && ((Struct) record.key()).getInt32("id") == 2
                    && ((Struct) record.value()).getString("op").equals("c");
        });

        final SourceRecord snapshotRecord = consumeRecord();
        assertThat(((Struct) snapshotRecord.key()).getInt32("id")).isEqualTo(1);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("UPDATE restart_table SET id = 2 WHERE id = 1");
        }

        final SourceRecord deleteBeforeStop = consumeRecord();
        assertThat(((Struct) deleteBeforeStop.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) deleteBeforeStop.value()).getString("op")).isEqualTo("d");

        if (!stopBeforeTombstone) {
            final SourceRecord tombstoneBeforeStop = consumeRecord();
            assertThat(((Struct) tombstoneBeforeStop.key()).getInt32("id")).isEqualTo(1);
            assertThat(tombstoneBeforeStop.value()).isNull();
        }

        waitForEngineShutdown();
        stopConnector();

        start(getConnectorClass(), config);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("INSERT INTO restart_table VALUES(3, 30)");
        }

        final SourceRecord replayedDelete = consumeRecord();
        assertThat(((Struct) replayedDelete.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) replayedDelete.value()).getString("op")).isEqualTo("d");

        final SourceRecord replayedTombstone = consumeRecord();
        assertThat(((Struct) replayedTombstone.key()).getInt32("id")).isEqualTo(1);
        assertThat(replayedTombstone.value()).isNull();

        final SourceRecord replayedCreate = consumeRecord();
        assertThat(((Struct) replayedCreate.key()).getInt32("id")).isEqualTo(2);
        assertThat(((Struct) replayedCreate.value()).getString("op")).isEqualTo("c");

        final SourceRecord markerCreate = consumeRecord();
        assertThat(((Struct) markerCreate.key()).getInt32("id")).isEqualTo(3);
        assertThat(((Struct) markerCreate.value()).getString("op")).isEqualTo("c");
    }

    @Test
    @FixFor("debezium/dbz#2549")
    public void shouldReplayPrimaryKeyUpdateWhenTombstonesAreDisabled() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("restart_table"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .with(BinlogConnectorConfig.MAX_BATCH_SIZE, 1)
                .build();

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute(
                    "CREATE TABLE restart_table (id INT PRIMARY KEY, val INT)",
                    "INSERT INTO restart_table VALUES(1, 10)");
        }

        start(getConnectorClass(), config, record -> record.value() != null
                && ((Struct) record.key()).getInt32("id") == 2
                && ((Struct) record.value()).getString("op").equals("c"));

        final SourceRecord snapshotRecord = consumeRecord();
        assertThat(((Struct) snapshotRecord.key()).getInt32("id")).isEqualTo(1);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("UPDATE restart_table SET id = 2 WHERE id = 1");
        }

        final SourceRecord deleteBeforeStop = consumeRecord();
        assertThat(((Struct) deleteBeforeStop.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) deleteBeforeStop.value()).getString("op")).isEqualTo("d");

        waitForEngineShutdown();
        stopConnector();

        start(getConnectorClass(), config);

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName());
                JdbcConnection connection = db.connect()) {
            connection.execute("INSERT INTO restart_table VALUES(3, 30)");
        }

        final SourceRecord replayedDelete = consumeRecord();
        assertThat(((Struct) replayedDelete.key()).getInt32("id")).isEqualTo(1);
        assertThat(((Struct) replayedDelete.value()).getString("op")).isEqualTo("d");

        final SourceRecord replayedCreate = consumeRecord();
        assertThat(((Struct) replayedCreate.key()).getInt32("id")).isEqualTo(2);
        assertThat(((Struct) replayedCreate.value()).getString("op")).isEqualTo("c");

        final SourceRecord markerCreate = consumeRecord();
        assertThat(((Struct) markerCreate.key()).getInt32("id")).isEqualTo(3);
        assertThat(((Struct) markerCreate.value()).getString("op")).isEqualTo("c");
    }
}

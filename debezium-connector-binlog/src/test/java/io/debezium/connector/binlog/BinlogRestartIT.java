/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    @FixFor("debezium/dbz#2738")
    public void shouldNotSkipEventsAfterRestartFollowingXaTransaction() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("xa_table"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 0)
                .build();
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE xa_table (id INT PRIMARY KEY)");

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(),
                "XA START 'xa1'", "INSERT INTO xa_table VALUES (1)", "XA END 'xa1'", "XA PREPARE 'xa1'", "XA COMMIT 'xa1'");
        assertThat(consumeIds(1)).containsExactly(1);
        stopConnector();

        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO xa_table VALUES (2)", "INSERT INTO xa_table VALUES (3)");
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        assertThat(consumeIds(2)).containsExactly(2, 3);
    }

    @Test
    @FixFor("debezium/dbz#2738")
    public void shouldResumeXaTransactionAfterRestart() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("xa_table"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 0)
                .build();
        executeStatements(DATABASE.getDatabaseName(), "CREATE TABLE xa_table (id INT PRIMARY KEY)");

        start(getConnectorClass(), config, record -> id(record) == 4);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        executeStatements(DATABASE.getDatabaseName(),
                "XA START 'xa2'", "INSERT INTO xa_table VALUES (1),(2)", "INSERT INTO xa_table VALUES (3),(4)", "XA END 'xa2'", "XA PREPARE 'xa2'", "XA COMMIT 'xa2'");
        assertThat(consumeIds(3)).containsExactly(1, 2, 3);
        waitForEngineShutdown();
        stopConnector();

        executeStatements(DATABASE.getDatabaseName(), "INSERT INTO xa_table VALUES (10)");
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
        assertThat(consumeIds(2)).containsExactly(4, 10);
    }

    private List<Integer> consumeIds(int count) throws InterruptedException {
        return consumeRecordsByTopic(count).allRecordsInOrder().stream().map(BinlogRestartIT::id).toList();
    }

    private static int id(SourceRecord record) {
        return ((Struct) record.value()).getStruct("after").getInt32("id");
    }
}

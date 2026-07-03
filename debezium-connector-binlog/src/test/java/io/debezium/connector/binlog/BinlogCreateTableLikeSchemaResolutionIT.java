/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.relational.history.SchemaHistory;

/**
 * @author leosanqing
 */
public abstract class BinlogCreateTableLikeSchemaResolutionIT<C extends SourceConnector>
        extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-create-table-like.txt")
            .toAbsolutePath();

    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("like_resolution_it", "create_table_like_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    private Configuration.Builder configWithCredentials(Configuration.Builder builder) {
        return builder
                .with(BinlogConnectorConfig.USER, System.getProperty("database.user", "snapper"))
                .with(BinlogConnectorConfig.PASSWORD, System.getProperty("database.password", "snapperpass"));
    }

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
    @FixFor("DBZ-248")
    public void shouldResolveCreateTableLikeWithFilteredReferenceTable() throws Exception {
        // Configure connector to capture only shard_* tables (exclude template_table).
        config = configWithCredentials(DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with("snapshot.locking.mode", "none")
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("shard_.*"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.RESOLVE_LIKE_TABLE_SCHEMA, true))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        // During streaming, execute CREATE TABLE shard_002 LIKE template_table
        // followed by an INSERT into shard_002.
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute(
                    "CREATE TABLE `shard_002` LIKE `template_table`",
                    "INSERT INTO `shard_002` (`name`, `status`) VALUES ('from_like_table', 2)");
        }

        waitForAvailableRecords(10, TimeUnit.SECONDS);
        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        List<SourceRecord> shard002Records = streamingRecords.recordsForTopic(DATABASE.topicForTable("shard_002"));
        assertThat(shard002Records).isNotNull().hasSize(1);

        Struct value = (Struct) shard002Records.get(0).value();
        assertThat(value.getString("op")).isEqualTo(Envelope.Operation.CREATE.code());

        Struct after = value.getStruct("after");
        assertThat(after.getString("name")).isEqualTo("from_like_table");
        assertThat(after.getInt32("status")).isEqualTo(2);

        stopConnector();
    }

    @Test
    @FixFor("DBZ-248")
    public void shouldResolveCreateTableLikeWithCrossDatabaseReference() throws Exception {
        // Test cross-database CREATE TABLE LIKE: shard_004 like <db>.template_table
        config = configWithCredentials(DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with("snapshot.locking.mode", "none")
                .with(BinlogConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("shard_.*"))
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.RESOLVE_LIKE_TABLE_SCHEMA, true))
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, true)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName(), getStreamingNamespace());

        String qualifiedRef = "`" + DATABASE.getDatabaseName() + "`.`template_table`";
        try (BinlogTestConnection connection = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            connection.execute(
                    "CREATE TABLE `shard_004` LIKE " + qualifiedRef,
                    "INSERT INTO `shard_004` (`name`, `status`) VALUES ('cross_db_ref', 4)");
        }

        // Should capture the INSERT because schema was resolved
        waitForAvailableRecords(10, TimeUnit.SECONDS);
        SourceRecords streamingRecords = consumeRecordsByTopic(1);
        List<SourceRecord> shard004Records = streamingRecords.recordsForTopic(DATABASE.topicForTable("shard_004"));
        assertThat(shard004Records).isNotNull().hasSize(1);

        Struct value = (Struct) shard004Records.get(0).value();
        assertThat(value.getString("op")).isEqualTo(Envelope.Operation.CREATE.code());

        Struct after = value.getStruct("after");
        assertThat(after.getString("name")).isEqualTo("cross_db_ref");
        assertThat(after.getInt32("status")).isEqualTo(4);

        stopConnector();
    }
}

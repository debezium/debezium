/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;

/**
 * Integration Tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 */
public abstract class BinlogSkipMessagesWithoutChangeConfigIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-decimal.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("skip_messages_db", "skip_messages_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabled() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.COLUMN_INCLUDE_LIST, getQualifiedColumnName("id") + "," + getQualifiedColumnName("white"))
                .with(BinlogConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=3, black=3 where id = 1");
            }
        }

        SourceRecords records = consumeRecordsByTopic(6);

        List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("debezium_test"));
        /*
         * Total Events:
         * Create Database
         * Create Table
         * 0,0,0 (I)
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        assertThat(recordsForTopic).hasSize(4);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(2);
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.get("white")).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.COLUMN_EXCLUDE_LIST, getQualifiedColumnName("black"))
                .with(BinlogConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=3, black=3 where id = 1");
            }
        }

        SourceRecords records = consumeRecordsByTopic(6);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("debezium_test"));
        /*
         * Total Events:
         * Create Database
         * Create Table
         * 0,0,0 (I)
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        assertThat(recordsForTopic).hasSize(4);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(2);
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.get("white")).isEqualTo(3);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInIncludedColumnsWhenSkipDisabled() throws Exception {
        config = DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.COLUMN_INCLUDE_LIST, getQualifiedColumnName("id") + "," + getQualifiedColumnName("white"))
                .with(BinlogConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, false)
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        try (BinlogTestConnection db = getTestDatabaseConnection(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=3, black=3 where id = 1");
            }
        }

        SourceRecords records = consumeRecordsByTopic(7);
        /*
         * Total Events:
         * Create Database
         * Create Table
         * 0,0,0 (I)
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("debezium_test"));
        assertThat(recordsForTopic).hasSize(5);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(1);
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.get("white")).isEqualTo(2);
        Struct fifthMessage = ((Struct) recordsForTopic.get(4).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(fifthMessage.get("white")).isEqualTo(3);
    }

    String getQualifiedColumnName(String column) {
        return String.format("%s.%s", DATABASE.qualifiedTableName("debezium_test"), column);
    }

}

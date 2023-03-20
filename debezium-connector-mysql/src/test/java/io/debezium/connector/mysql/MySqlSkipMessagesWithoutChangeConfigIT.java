/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Integration Tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 */
public class MySqlSkipMessagesWithoutChangeConfigIT extends AbstractConnectorTest {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-decimal.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("skip_messages_db", "skip_messages_test")
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
    public void shouldSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipEnabled() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.COLUMN_INCLUDE_LIST, getQualifiedColumnName("id") + "," + getQualifiedColumnName("white"))
                .with(MySqlConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", DATABASE.getServerName());

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
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
         */
        assertThat(recordsForTopic).hasSize(3);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.COLUMN_EXCLUDE_LIST, getQualifiedColumnName("black"))
                .with(MySqlConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", DATABASE.getServerName());

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
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
         */
        assertThat(recordsForTopic).hasSize(3);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(2);
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInWhitelistedColumnsWhenSkipDisabled() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.COLUMN_INCLUDE_LIST, getQualifiedColumnName("id") + "," + getQualifiedColumnName("white"))
                .with(MySqlConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, false)
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NEVER)
                .build();

        start(MySqlConnector.class, config);
        waitForStreamingRunning("mysql", DATABASE.getServerName());

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("insert into debezium_test(id, black, white) values(1, 1, 1)");
                connection.execute("UPDATE debezium_test SET black=2 where id = 1");
                connection.execute("UPDATE debezium_test SET white=2 where id = 1");
            }
        }

        SourceRecords records = consumeRecordsByTopic(6);
        /*
         * Total Events:
         * Create Database
         * Create Table
         * 0,0,0 (I)
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         */
        List<SourceRecord> recordsForTopic = records.recordsForTopic(DATABASE.topicForTable("debezium_test"));
        assertThat(recordsForTopic).hasSize(4);
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("white")).isEqualTo(1);
    }

    String getQualifiedColumnName(String column) {
        return String.format("%s.%s", DATABASE.qualifiedTableName("debezium_test"), column);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Integration Tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 *
 */
public class SqlServerSkipMessagesWithNoUpdateConfigIT extends AbstractConnectorTest {
    private SqlServerConnection connection;

    private final Configuration.Builder configBuilder = TestHelper.defaultConfig()
            .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
            .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.skip_messages_test")
            .with(SqlServerConnectorConfig.COLUMN_INCLUDE_LIST, "dbo.skip_messages_test.id, dbo.skip_messages_test.white");

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        connection.execute(
                "CREATE TABLE skip_messages_test (id int primary key, white int, black int)");
        TestHelper.enableTableCdc(connection, "skip_messages_test");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabled() throws Exception {
        Configuration config = configBuilder
                .with(SqlServerConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .build();

        start(SqlServerConnector.class, config);
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO skip_messages_test VALUES (1, 1, 1);");
        connection.execute("UPDATE skip_messages_test SET black=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=3,black=3 where id=1");
        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableMessages = records.recordsForTopic("server1.testDB1.dbo.skip_messages_test");
        /*
         * Total events:
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        assertThat(tableMessages).hasSize(3);
        final Struct secondMessage = (Struct) tableMessages.get(1).value();
        assertThat(((Struct) secondMessage.get("after")).get("white")).isEqualTo(2);
        final Struct thirdMessage = (Struct) tableMessages.get(2).value();
        assertThat(((Struct) thirdMessage.get("after")).get("white")).isEqualTo(3);
        stopConnector();
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo.skip_messages_test")
                .with(SqlServerConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(SqlServerConnectorConfig.COLUMN_EXCLUDE_LIST, "dbo.skip_messages_test.black")
                .build();

        start(SqlServerConnector.class, config);
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO skip_messages_test VALUES (1, 1, 1);");
        connection.execute("UPDATE skip_messages_test SET black=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=3,black=3 where id=1");
        final SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> tableMessages = records.recordsForTopic("server1.testDB1.dbo.skip_messages_test");
        /*
         * Total events:
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        assertThat(tableMessages).hasSize(3);
        final Struct secondMessage = (Struct) tableMessages.get(1).value();
        assertThat(((Struct) secondMessage.get("after")).get("white")).isEqualTo(2);
        final Struct thirdMessage = (Struct) tableMessages.get(2).value();
        assertThat(((Struct) thirdMessage.get("after")).get("white")).isEqualTo(3);
        stopConnector();
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInIncludedColumnsWhenSkipDisabled() throws Exception {
        Configuration config = configBuilder
                .with(SqlServerConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, false)
                .build();

        start(SqlServerConnector.class, config);
        TestHelper.waitForStreamingStarted();

        connection.execute("INSERT INTO skip_messages_test VALUES (1, 1, 1);");
        connection.execute("UPDATE skip_messages_test SET black=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=2 where id=1");
        connection.execute("UPDATE skip_messages_test SET white=3,black=3 where id=1");
        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableMessages = records.recordsForTopic("server1.testDB1.dbo.skip_messages_test");
        /*
         * Total events:
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        assertThat(tableMessages).hasSize(4);
        final Struct secondMessage = (Struct) tableMessages.get(1).value();
        assertThat(((Struct) secondMessage.get("after")).get("white")).isEqualTo(1);
        final Struct thirdMessage = (Struct) tableMessages.get(2).value();
        assertThat(((Struct) thirdMessage.get("after")).get("white")).isEqualTo(2);
        final Struct forthMessage = (Struct) tableMessages.get(3).value();
        assertThat(((Struct) forthMessage.get("after")).get("white")).isEqualTo(3);
        stopConnector();
    }

}

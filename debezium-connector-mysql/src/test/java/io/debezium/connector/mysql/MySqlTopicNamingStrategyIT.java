/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.schema.DefaultRegexTopicNamingStrategy;
import io.debezium.schema.DefaultUnicodeTopicNamingStrategy;
import io.debezium.util.Testing;

public class MySqlTopicNamingStrategyIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE_NAME = "dbz4180";
    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-comment.txt")
            .toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("topic_strategy", "strategy_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-4180")
    public void testSpecifyDelimiterAndPrefixStrategy() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, "true")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "my_prefix")
                .with(AbstractTopicNamingStrategy.TOPIC_DELIMITER, "_")
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(100);

        String expectedDataTopic = String.join("_", "my_prefix", DATABASE.getDatabaseName(), TABLE_NAME);
        List<SourceRecord> dataChangeEvents = records.recordsForTopic(expectedDataTopic);
        assertThat(dataChangeEvents.size()).isEqualTo(1);

        String expectedSchemaTopic = "my_prefix";
        List<SourceRecord> schemaChangeEvents = records.recordsForTopic(expectedSchemaTopic);
        assertThat(schemaChangeEvents.size()).isEqualTo(12);

        // insert data
        try (Connection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName()).connection()) {
            conn.createStatement().execute("INSERT INTO dbz4180(a, b, c, d) VALUE (10.1, 10.2, 'strategy 1', 1290)");
        }

        dataChangeEvents = consumeRecordsByTopic(1).recordsForTopic(expectedDataTopic);
        assertThat(dataChangeEvents.size()).isEqualTo(1);
        SourceRecord sourceRecord = dataChangeEvents.get(0);
        final Struct change = ((Struct) sourceRecord.value()).getStruct("after");
        assertThat(change.getString("c")).isEqualTo("strategy 1");

        stopConnector();
    }

    @Test
    @FixFor("DBZ-4180")
    public void testSpecifyByLogicalTableStrategy() throws SQLException, InterruptedException {
        String tables = DATABASE.qualifiedTableName("dbz_4180_00") + "," + DATABASE.qualifiedTableName("dbz_4180_01");
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, tables)
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, "false")
                .with(DefaultRegexTopicNamingStrategy.TOPIC_REGEX, "(.*)(dbz_4180)(.*)")
                .with(DefaultRegexTopicNamingStrategy.TOPIC_REPLACEMENT, "$1$2_all_shards")
                .with(DefaultRegexTopicNamingStrategy.TOPIC_KEY_FIELD_NAME, "origin_table_name")
                .with(DefaultRegexTopicNamingStrategy.TOPIC_KEY_FIELD_REGEX, "(.*)")
                .with(DefaultRegexTopicNamingStrategy.TOPIC_KEY_FIELD_REPLACEMENT, "it_$1")
                .with(CommonConnectorConfig.TOPIC_NAMING_STRATEGY, "io.debezium.schema.DefaultRegexTopicNamingStrategy")
                .build();

        start(MySqlConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // insert data
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                String shard0 = "INSERT INTO dbz_4180_00(a, b, c, d) VALUE (10.1, 10.2, 'shard 0', 10);";
                String shard1 = "INSERT INTO dbz_4180_01(a, b, c, d) VALUE (10.1, 10.2, 'shard 1', 11);";
                conn.execute(shard0, shard1);
            }
        }

        String expectedTopic = DATABASE.topicForTable("dbz_4180_all_shards");
        SourceRecords sourceRecords = consumeRecordsByTopic(100);
        List<SourceRecord> records = sourceRecords.recordsForTopic(expectedTopic);
        SourceRecord record = records.get(0);
        assertThat(record.keySchema().field("origin_table_name")).isNotNull();
        assertThat(((Struct) record.key()).get("origin_table_name").toString().startsWith("it_")).isTrue();
        assertEquals(2, records.size());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-4180")
    public void testSpecifyTransactionStrategy() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME))
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, "false")
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, "true")
                .with(AbstractTopicNamingStrategy.TOPIC_TRANSACTION, "my_transaction")
                .build();

        start(MySqlConnector.class, config);

        // Testing.Debug.enable();
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // insert data
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.setAutoCommit(false);
                conn.execute("INSERT INTO dbz4180(a, b, c, d) VALUE (10.1, 10.2, 'test transaction', 1290)");
                conn.commit();
            }
        }

        SourceRecords sourceRecords = consumeRecordsByTopic(100);
        String expectedTransactionTopic = DATABASE.getServerName() + "." + "my_transaction";
        List<SourceRecord> transactionRecords = sourceRecords.recordsForTopic(expectedTransactionTopic);
        assertEquals(2, transactionRecords.size());

        List<SourceRecord> records = sourceRecords.allRecordsInOrder();
        // BEGIN + 1 INSERT + END
        assertEquals(1 + 1 + 1, records.size());

        stopConnector();
    }

    @Test
    @FixFor("DBZ-5743")
    public void testUnicodeTopicNamingStrategy() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("dbz5743中文"))
                .with(RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES, "true")
                .with(CommonConnectorConfig.TOPIC_NAMING_STRATEGY, "io.debezium.schema.DefaultUnicodeTopicNamingStrategy")
                .build();

        start(MySqlConnector.class, config);

        assertConnectorIsRunning();

        String tableName = String.join(".", DATABASE.getDatabaseName(), "dbz5743中文");
        DefaultUnicodeTopicNamingStrategy strategy = new DefaultUnicodeTopicNamingStrategy(config.asProperties());
        String expectedDataTopic = strategy.dataChangeTopic(TableId.parse(tableName));

        SourceRecords sourceRecords = consumeRecordsByTopic(100);
        List<SourceRecord> dataChangeEvents = sourceRecords.recordsForTopic(expectedDataTopic);
        assertEquals(1, dataChangeEvents.size());

        stopConnector();
    }
}

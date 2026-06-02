/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.connector.binlog.junit.SkipWhenGtidModeIs;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * Integration test for MySQL GTID tags support (MySQL 8.3+).
 *
 * GTID tags allow tagging transactions with custom identifiers in the format
 * `uuid:tag:transaction_id` instead of the legacy `uuid:transaction_id` format.
 *
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = EqualityCheck.LESS_THAN, major = 8, minor = 3, reason = "GTID tags are only supported in MySQL 8.3+")
@SkipWhenGtidModeIs(value = SkipWhenGtidModeIs.GtidMode.OFF)
public class MySqlGtidTagsIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-gtid-tags.txt")
            .toAbsolutePath();
    private static final String GTID_TAG = "debezium_test";

    private UniqueDatabase database;
    private Configuration config;

    @BeforeEach
    void beforeEach() throws SQLException {
        stopConnector();
        Testing.Files.delete(OFFSET_STORE_PATH);
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
        database = TestHelper.getUniqueDatabase("gtid_tags_server", "gtid_tags_test")
                .withDbHistoryPath(SCHEMA_HISTORY_PATH);
        database.create();
        initializeConnectorTestFramework();
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(OFFSET_STORE_PATH);
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#1200")
    void shouldStreamChangesWithGtidTags() throws SQLException, InterruptedException {
        // Configure connector for streaming with GTID enabled
        config = database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Create a test table
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                statement.execute("DROP TABLE IF EXISTS gtid_tags_test");
                statement.execute("""
                        CREATE TABLE gtid_tags_test (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(50)
                        )
                        """);
            }
        }

        // Start the connector
        start(MySqlConnector.class, config);

        // Wait for snapshot to complete
        waitForSnapshotToBeCompleted(getConnectorName(), database.getServerName());

        // Insert data with GTID tag
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            // Enable GTID tags for this session (MySQL 8.3+)
            try (var statement = db.connection().createStatement()) {
                // Set GTID tag for subsequent transactions
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC:%s'".formatted(GTID_TAG));
                statement.execute("INSERT INTO gtid_tags_test (name) VALUES ('test_record_1')");
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC'");
            }
        }

        // Consume the insert record
        final List<SourceRecord> tableRecords = consumeTableRecords("gtid_tags_test", 1);

        final SourceRecord record = tableRecords.get(0);
        assertThat(record).isNotNull();

        // Verify the record contains the expected data
        assertInsert(record, "id", 1);
        assertValueField(record, "after/name", "test_record_1");
        assertGtidTag(record, GTID_TAG);

        // Insert another record without explicit tag
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                statement.execute("INSERT INTO gtid_tags_test (name) VALUES ('test_record_2')");
            }
        }

        // Consume the second insert record
        final List<SourceRecord> tableRecords2 = consumeTableRecords("gtid_tags_test", 1);

        final SourceRecord record2 = tableRecords2.get(0);
        assertThat(record2).isNotNull();
        assertInsert(record2, "id", 2);
        assertValueField(record2, "after/name", "test_record_2");
        assertUntaggedGtid(record2);
    }

    @Test
    @FixFor("debezium/dbz#1200")
    void shouldHandleMultipleGtidTagsInStream() throws SQLException, InterruptedException {
        // Configure connector for streaming
        config = database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NO_DATA)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        // Create a test table
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                statement.execute("DROP TABLE IF EXISTS multi_tags_test");
                statement.execute("""
                        CREATE TABLE multi_tags_test (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            tag_name VARCHAR(50),
                            value INT
                        )
                        """);
            }
        }

        // Start the connector
        start(MySqlConnector.class, config);

        // Wait for streaming to start
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        // Insert records with different GTID tags
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                // First tag
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC:tag1'");
                statement.execute("INSERT INTO multi_tags_test (tag_name, value) VALUES ('tag1', 100)");
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC'");

                // Second tag
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC:tag2'");
                statement.execute("INSERT INTO multi_tags_test (tag_name, value) VALUES ('tag2', 200)");
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC'");

                // No explicit tag
                statement.execute("INSERT INTO multi_tags_test (tag_name, value) VALUES ('notag', 300)");
            }
        }

        // Consume all three insert records
        final List<SourceRecord> tableRecords = consumeTableRecords("multi_tags_test", 3);

        // Verify first record
        final SourceRecord record1 = tableRecords.get(0);
        assertInsert(record1, "id", 1);
        assertValueField(record1, "after/tag_name", "tag1");
        assertValueField(record1, "after/value", 100);
        assertGtidTag(record1, "tag1");

        // Verify second record
        final SourceRecord record2 = tableRecords.get(1);
        assertInsert(record2, "id", 2);
        assertValueField(record2, "after/tag_name", "tag2");
        assertValueField(record2, "after/value", 200);
        assertGtidTag(record2, "tag2");

        // Verify third record
        final SourceRecord record3 = tableRecords.get(2);
        assertInsert(record3, "id", 3);
        assertValueField(record3, "after/tag_name", "notag");
        assertValueField(record3, "after/value", 300);
        assertUntaggedGtid(record3);

        Testing.print("Successfully streamed %s records with GTID tags".formatted(tableRecords.size()));
    }

    @Test
    @FixFor("debezium/dbz#1200")
    void shouldRestartFromTaggedGtid() throws SQLException, InterruptedException {
        config = database.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.NO_DATA)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .build();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                statement.execute("DROP TABLE IF EXISTS gtid_tags_restart_test");
                statement.execute("""
                        CREATE TABLE gtid_tags_restart_test (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(50)
                        )
                        """);
            }
        }

        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted(getConnectorName(), database.getServerName());
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        insertWithGtidTag("gtid_tags_restart_test", "before_restart", "restart_before");

        final SourceRecord firstRecord = consumeTableRecords("gtid_tags_restart_test", 1).get(0);
        assertInsert(firstRecord, "id", 1);
        assertValueField(firstRecord, "after/name", "before_restart");
        assertGtidTag(firstRecord, "restart_before");

        insertWithGtidTag("gtid_tags_restart_test", "offset_barrier", "restart_barrier");

        final SourceRecord barrierRecord = consumeTableRecords("gtid_tags_restart_test", 1).get(0);
        assertInsert(barrierRecord, "id", 2);
        assertValueField(barrierRecord, "after/name", "offset_barrier");
        assertGtidTag(barrierRecord, "restart_barrier");

        waitForOffsetStoreToContain("restart_before");

        stopConnector();

        start(MySqlConnector.class, config);
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        insertWithGtidTag("gtid_tags_restart_test", "after_restart", "restart_after");

        final List<SourceRecord> restartRecords = consumeTableRecordsUntil("gtid_tags_restart_test", "after_restart");
        assertThat(restartRecords)
                .extracting(record -> afterValue(record, "name"))
                .doesNotContain("before_restart");

        final SourceRecord restartRecord = restartRecords.get(restartRecords.size() - 1);
        assertInsert(restartRecord, "id", 3);
        assertValueField(restartRecord, "after/name", "after_restart");
        assertGtidTag(restartRecord, "restart_after");
    }

    private List<SourceRecord> consumeTableRecords(String tableName, int expectedRecordCount) throws InterruptedException {
        final List<SourceRecord> records = new ArrayList<>();

        Awaitility.await()
                .atMost(waitTimeForRecords() * 10L, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
                    drainTableRecords(tableName, records);
                    assertThat(records).hasSizeGreaterThanOrEqualTo(expectedRecordCount);
                });

        if (records.size() > expectedRecordCount) {
            return new ArrayList<>(records.subList(0, expectedRecordCount));
        }
        return records;
    }

    private List<SourceRecord> consumeTableRecordsUntil(String tableName, String expectedName) throws InterruptedException {
        final List<SourceRecord> records = new ArrayList<>();

        Awaitility.await()
                .atMost(waitTimeForRecords() * 10L, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
                    drainTableRecords(tableName, records);
                    assertThat(records).isNotEmpty();
                    assertThat(afterValue(records.get(records.size() - 1), "name")).isEqualTo(expectedName);
                });
        return records;
    }

    private void drainTableRecords(String tableName, List<SourceRecord> records) throws InterruptedException {
        final String topicName = database.topicForTable(tableName);
        consumeAvailableRecords(record -> {
            if (topicName.equals(record.topic())) {
                records.add(record);
            }
        });
    }

    private Object afterValue(SourceRecord record, String fieldName) {
        final Struct value = (Struct) record.value();
        final Struct after = value.getStruct("after");
        return after.get(fieldName);
    }

    private void waitForOffsetStoreToContain(String value) throws InterruptedException {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 30L, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(offsetStoreContent()).contains(value));
    }

    private String offsetStoreContent() {
        try {
            return java.nio.file.Files.exists(OFFSET_STORE_PATH) ? java.nio.file.Files.readString(OFFSET_STORE_PATH, StandardCharsets.ISO_8859_1) : "";
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void insertWithGtidTag(String tableName, String name, String tag) throws SQLException {
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(database.getDatabaseName())) {
            try (var statement = db.connection().createStatement()) {
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC:%s'".formatted(tag));
                statement.execute("INSERT INTO %s (name) VALUES ('%s')".formatted(tableName, name));
                statement.execute("SET @@SESSION.gtid_next = 'AUTOMATIC'");
            }
        }
    }

    private void assertGtidTag(SourceRecord record, String expectedTag) {
        final String gtid = sourceGtid(record);
        final String[] parts = gtid.split(":", 3);
        assertThat(parts).hasSize(3);
        assertThat(parts[0]).matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
        assertThat(parts[1]).isEqualTo(expectedTag);
        assertThat(parts[2]).isEqualTo("1");
    }

    private void assertUntaggedGtid(SourceRecord record) {
        final String gtid = sourceGtid(record);
        final String[] parts = gtid.split(":", 3);
        assertThat(parts).hasSize(2);
        assertThat(parts[0]).matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
        assertThat(parts[1]).matches("[0-9]+");
        assertThat(record.sourceOffset().get(BinlogOffsetContext.GTID_SET_KEY)).isNotNull();
    }

    private String sourceGtid(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = value.getStruct("source");
        return source.getString(BinlogSourceInfo.GTID_KEY);
    }

}

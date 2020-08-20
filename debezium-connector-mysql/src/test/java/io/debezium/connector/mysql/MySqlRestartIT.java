/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

/**
 * @author Jiri Pechanec
 */
public class MySqlRestartIT extends AbstractConnectorTest {
    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-restart.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("restart", "connector_test").withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("DBZ-1276")
    public void shouldNotDuplicateEventsAfterRestart() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName("restart_table"))
                .build();

        try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute(
                        "CREATE TABLE restart_table (id INT PRIMARY KEY, val INT)",
                        "INSERT INTO restart_table VALUES(1, 10)");
            }
        }
        start(MySqlConnector.class, config, record -> {
            final Schema schema = record.valueSchema();
            final Struct value = ((Struct) record.value());
            return schema.field("after") != null && value.getStruct("after").getInt32("id").equals(5);
        });

        // Testing.Print.enable();

        SourceRecords records = consumeRecordsByTopic(15);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("restart_table")).size()).isEqualTo(1);

        try (MySQLConnection db = MySQLConnection.forTestDatabase(DATABASE.getDatabaseName());) {
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

        stopConnector();

        start(MySqlConnector.class, config);

        records = consumeRecordsByTopic(2);
        assertThat(records.recordsForTopic(DATABASE.topicForTable("restart_table")).size()).isEqualTo(2);
        assertThat(((Struct) ((SourceRecord) records.recordsForTopic(DATABASE.topicForTable("restart_table")).get(0)).value()).getStruct("after").getInt32("id"))
                .isEqualTo(5);

        stopConnector();
    }
}

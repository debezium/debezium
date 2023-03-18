/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;

/**
 * Tests for the MySQL {@code REAL} data type synonym.
 *
 * @author Chris Cranford
 */
public class MySqlRealIT extends AbstractConnectorTest {

    private static final String TABLE_NAME = "REAL_TEST";
    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-boolean.text").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("realit", "real_test").withDbHistoryPath(SCHEMA_HISTORY_PATH);

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
    @FixFor("DBZ-6226")
    public void testRealDataTypeMappedConsistentlyBetweenSnapshotAndStreamingAfterTheFact() throws Exception {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, DATABASE.qualifiedTableName(TABLE_NAME) + "," + DATABASE.qualifiedTableName("REAL_TEST2"))
                .with(MySqlConnectorConfig.PROPAGATE_COLUMN_SOURCE_TYPE, ".*")
                .build();

        start(MySqlConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(2 + 4 + 1);
        assertThat(records).isNotNull();

        List<SourceRecord> tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME));
        assertThat(tableRecords).hasSize(1);

        SourceRecord record = tableRecords.get(0);

        Schema after = record.valueSchema().field("after").schema();

        // Assert how the BOOLEAN data type is mapped during the snapshot phase.
        assertThat(after.field("r1").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(after.field("r1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("DOUBLE");

        // Create the table after-the-fact
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection conn = db.connect()) {
                conn.execute("CREATE TABLE REAL_TEST2 (`id` INT NOT NULL AUTO_INCREMENT, " +
                        "`r1` real default 3.14, " +
                        "primary key (`ID`)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;");
                conn.execute("INSERT INTO REAL_TEST2 (r1) VALUES (9.78)");
            }
        }

        records = consumeRecordsByTopic(2);
        assertThat(records).isNotNull();

        tableRecords = records.recordsForTopic(DATABASE.topicForTable(TABLE_NAME + "2"));
        assertThat(tableRecords).hasSize(1);

        record = tableRecords.get(0);

        after = record.valueSchema().field("after").schema();

        // Assert the created table after-the-fact record is identical to the snapshot
        assertThat(after.field("r1").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(after.field("r1").schema().parameters().get("__debezium.source.column.type")).isEqualTo("DOUBLE");

        stopConnector();
    }
}
